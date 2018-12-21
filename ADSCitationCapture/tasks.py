from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
import ADSCitationCapture.app as app_module
import ADSCitationCapture.webhook as webhook
import ADSCitationCapture.doi as doi
import ADSCitationCapture.url as url
import ADSCitationCapture.db as db
import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCitationCaptureCelery('ads-citation-capture', proj_home=proj_home)
logger = app.logger


app.conf.CELERY_QUEUES = (
    Queue('process-citation-changes', app.exchange, routing_key='process-citation-changes'),
    Queue('process-new-citation', app.exchange, routing_key='process-new-citation'),
    Queue('process-updated-citation', app.exchange, routing_key='process-updated-citation'),
    Queue('process-deleted-citation', app.exchange, routing_key='process-deleted-citation'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)


# ============================= TASKS ============================================= #

@app.task(queue='process-new-citation')
def task_process_new_citation(citation_change, force=False):
    """
    """
    content_type = None
    is_link_alive = False
    status = "DISCARDED"

    # Check if we already have the citation target in the DB
    metadata = db.get_citation_target_metadata(app, citation_change)
    citation_target_in_db = bool(metadata) # False if dict is empty
    raw_metadata = metadata.get('raw', None)
    parsed_metadata = metadata.get('parsed', None)
    if parsed_metadata and parsed_metadata.get('bibcode') not in (None, ""):
        status = "REGISTERED"

    if citation_change.content_type == adsmsg.CitationChangeContentType.doi \
        and citation_change.content not in ["", None]:
        # Default values
        content_type = "DOI"
        #
        if not citation_target_in_db:
            # Fetch DOI metadata (if HTTP request fails, an exception is raised
            # and the task will be re-queued (see app.py and adsputils))
            raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], citation_change.content)
            if raw_metadata:
                parsed_metadata = doi.parse_metadata(raw_metadata)
                if parsed_metadata.get('bibcode') not in (None, ""):
                    status = "REGISTERED"
    elif citation_change.content_type == adsmsg.CitationChangeContentType.pid \
        and citation_change.content not in ["", None]:
        content_type = "PID"
        status = None
        is_link_alive = url.is_alive(app.conf['ASCL_URL'] + citation_change.content)
        parsed_metadata = {'link_alive': is_link_alive }
    elif citation_change.content_type == adsmsg.CitationChangeContentType.url \
        and citation_change.content not in ["", None]:
        content_type = "URL"
        status = None
        is_link_alive = url.is_alive(citation_change.content)
        parsed_metadata = {'link_alive': is_link_alive }
    else:
        logger.error("Citation change should have doi, pid or url informed: {}", citation_change)
        status = None

    if status is not None:
        if not citation_target_in_db:
            # Create citation target in the DB
            target_stored = db.store_citation_target(app, citation_change, content_type, raw_metadata, parsed_metadata, status)
        stored = db.store_citation(app, citation_change, content_type, raw_metadata, parsed_metadata, status)
        if stored:
            logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
            task_emit_event.delay(citation_change, parsed_metadata)

@app.task(queue='process-updated-citation')
def task_process_updated_citation(citation_change, force=False):
    """
    Update citation record unless the record it is DELETED
    """
    updated = db.update_citation(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change)
    parsed_metadata = metadata.get('parsed', None)
    if updated:
        logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
        task_emit_event.delay(citation_change, parsed_metadata)

@app.task(queue='process-deleted-citation')
def task_process_deleted_citation(citation_change, force=False):
    """
    Mark a citation as deleted
    """
    marked_as_deleted = db.mark_citation_as_deleted(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change)
    parsed_metadata = metadata.get('parsed', None)
    if marked_as_deleted:
        logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
        task_emit_event.delay(citation_change, parsed_metadata)


@app.task(queue='process-emit-event')
def task_emit_event(citation_change, parsed_metadata):
    """
    Emit event
    """
    emitted = False
    is_link_alive = parsed_metadata and parsed_metadata.get("link_alive", False)
    is_software = parsed_metadata and parsed_metadata.get("doctype", "").lower() == "software"
    if is_software and is_link_alive:
        emitted = webhook.emit_event(app.conf['ADS_WEBHOOK_URL'], app.conf['ADS_WEBHOOK_AUTH_TOKEN'], citation_change)

    if emitted:
        logger.debug("Emitted '%s'", citation_change)
    else:
        logger.debug("Not emitted '%s'", citation_change)

@app.task(queue='process-citation-changes')
def task_process_citation_changes(citation_changes, force=False):
    """
    Process citation changes
    """
    logger.debug('Checking content: %s', citation_changes)
    for citation_change in citation_changes.changes:
        # Check: Is this citation already stored in the DB?
        citation_in_db = db.citation_already_exists(app, citation_change)

        if citation_change.status == adsmsg.Status.new:
            if citation_in_db:
                logger.error("Ignoring new citation (citting '%s', content '%s' and timestamp '%s') because it already exists in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_new_citation' with '%s'", citation_change)
                task_process_new_citation.delay(citation_change, force=force)
        elif citation_change.status == adsmsg.Status.updated:
            if not citation_in_db:
                logger.error("Ignoring updated citation (citting '%s', content '%s' and timestamp '%s') because it does not exist in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_updated_citation' with '%s'", citation_change)
                task_process_updated_citation.delay(citation_change, force=False)
        elif citation_change.status == adsmsg.Status.deleted:
            if not citation_in_db:
                logger.error("Ignoring deleted citation (citting '%s', content '%s' and timestamp '%s') because it does not exist in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_deleted_citation' with '%s'", citation_change)
                task_process_deleted_citation.delay(citation_change)


        #logger.debug("Calling 'task_output_results' with '%s'", citation_change)
        ##task_output_results.delay(citation_change)
        #task_output_results(citation_change)

@app.task(queue='output-results')
def task_output_results(citation_change):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param citation_change: contains citation changes
    :return: no return
    """
    logger.debug('Will forward this record: %s', citation_change)
    logger.debug("Calling 'app.forward_message' with '%s'", str(citation_change))
    app.forward_message(citation_change)



if __name__ == '__main__':
    app.start()
