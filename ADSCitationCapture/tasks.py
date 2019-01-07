from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
from google.protobuf.json_format import MessageToDict
import ADSCitationCapture.app as app_module
import ADSCitationCapture.webhook as webhook
import ADSCitationCapture.doi as doi
import ADSCitationCapture.url as url
import ADSCitationCapture.db as db
import ADSCitationCapture.forward as forward
import ADSCitationCapture.api as api
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
    Queue('maintenance', app.exchange, routing_key='maintenance'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)


# ============================= TASKS ============================================= #

@app.task(queue='process-new-citation')
def task_process_new_citation(citation_change, force=False):
    """
    """
    content_type = None
    is_link_alive = False
    status = u"DISCARDED"

    # Check if we already have the citation target in the DB
    metadata = db.get_citation_target_metadata(app, citation_change)
    citation_target_in_db = bool(metadata) # False if dict is empty
    raw_metadata = metadata.get('raw', None)
    parsed_metadata = metadata.get('parsed', None)
    if citation_target_in_db:
        status = metadata.get('status', u'DISCARDED') # "REGISTERED" if it is a software record

    if citation_change.content_type == adsmsg.CitationChangeContentType.doi \
        and citation_change.content not in ["", None]:
        # Default values
        content_type = u"DOI"
        #
        if not citation_target_in_db:
            # Fetch DOI metadata (if HTTP request fails, an exception is raised
            # and the task will be re-queued (see app.py and adsputils))
            raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], citation_change.content)
            if raw_metadata:
                parsed_metadata = doi.parse_metadata(raw_metadata)
                is_software = parsed_metadata.get('doctype', u'').lower() == "software"
                if parsed_metadata.get('bibcode') not in (None, "") and is_software:
                    status = u"REGISTERED"
    elif citation_change.content_type == adsmsg.CitationChangeContentType.pid \
        and citation_change.content not in ["", None]:
        content_type = u"PID"
        status = None
        is_link_alive = url.is_alive(app.conf['ASCL_URL'] + citation_change.content)
        parsed_metadata = {'link_alive': is_link_alive }
    elif citation_change.content_type == adsmsg.CitationChangeContentType.url \
        and citation_change.content not in ["", None]:
        content_type = u"URL"
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
        if status == u"REGISTERED":
            if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
                canonical_citing_bibcode = api.get_canonical_bibcode(app, citation_change.citing)
                citation_target_bibcode = parsed_metadata.get('bibcode')
                citations = _get_citations(app, citation_target_bibcode)
                # Clean before adding the current citation
                citations = [c for c in citations if c != citation_change.citing and c != canonical_citing_bibcode]
                # Add canonical bibcode of current detected citation
                if canonical_citing_bibcode:
                    citations.append(canonical_citing_bibcode)
                else:
                    citations.append(citation_change.citing)
                logger.debug("Calling 'task_output_results' with '%s'", citation_change)
                task_output_results.delay(citation_change, parsed_metadata, citations)
            logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
            task_emit_event.delay(citation_change, parsed_metadata)
        # Store the citation at the very end, so that if an exception is raised before
        # this task can be re-run in the future without key collisions in the database
        stored = db.store_citation(app, citation_change, content_type, raw_metadata, parsed_metadata, status)

def _get_citations(app, bibcode):
    """
    Get citations for a bibcode from Solr or, if it does not exist in solr, get
    them from the Citation Capture database and transform the stored bibcodes
    into their canonical ones as registered in Solr.
    """
    # Check if the citation target already exists in Solr
    bibcode = api.get_canonical_bibcode(app, bibcode)
    if bibcode:
        # It exists, then use the list of citations as provided by
        # ADS API which already merged duplicated records and removed invalid bibcodes
        existing_citation_bibcodes = api.request_existing_citations(app, bibcode)
        citations = existing_citation_bibcodes
    else:
        # It does not exist, use Citation Capture database
        original_citations = db.get_citations_by_bibcode(app, bibcode)
        # Transform citation bibcodes into their canonical form
        citations = api.get_canonical_bibcodes(app, original_citations)
    return list(set(citations))

@app.task(queue='process-updated-citation')
def task_process_updated_citation(citation_change, force=False):
    """
    Update citation record unless the record it is DELETED
    """
    updated = db.update_citation(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change)
    parsed_metadata = metadata.get('parsed', {})
    citation_target_bibcode = parsed_metadata.get('bibcode')
    if updated:
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
            citations = _get_citations(app, citation_target_bibcode)
            logger.debug("Calling 'task_output_results' with '%s'", citation_change)
            task_output_results.delay(citation_change, parsed_metadata, citations)
        logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
        task_emit_event.delay(citation_change, parsed_metadata)

@app.task(queue='process-deleted-citation')
def task_process_deleted_citation(citation_change, force=False):
    """
    Mark a citation as deleted
    """
    marked_as_deleted = db.mark_citation_as_deleted(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change)
    parsed_metadata = metadata.get('parsed', {})
    citation_target_bibcode = parsed_metadata.get('bibcode')
    if marked_as_deleted:
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
            citations = _get_citations(app, citation_target_bibcode)
            logger.debug("Calling 'task_output_results' with '%s'", citation_change)
            task_output_results.delay(citation_change, parsed_metadata, citations)
        logger.debug("Calling 'task_emit_event' with '%s'", citation_change)
        task_emit_event.delay(citation_change, parsed_metadata)

def _protobuf_to_adsmsg_citation_change(pure_protobuf):
    """
    Transforms pure citation_change protobuf to adsmsg.CitationChange,
    which can be safely sent via Celery/RabbitMQ.
    """
    tmp = MessageToDict(pure_protobuf, preserving_proto_field_name=True)
    if 'content_type' in tmp:
        # Convert content_type from string to value
        tmp['content_type'] = getattr(adsmsg.CitationChangeContentType, tmp['content_type'])
    else:
        tmp['content_type'] = 0 # default: adsmsg.CitationChangeContentType.doi
    if 'timestamp' in tmp:
        # Ignore original protobuf timestamps since that value is set when the
        # protobuf is created
        del tmp['timestamp']
    return adsmsg.CitationChange(**tmp)

@app.task(queue='process-citation-changes')
def task_process_citation_changes(citation_changes, force=False):
    """
    Process citation changes
    """
    logger.debug('Checking content: %s', citation_changes)
    for citation_change in citation_changes.changes:
        citation_change = _protobuf_to_adsmsg_citation_change(citation_change)
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
        #emitted = True

    if emitted:
        logger.debug("Emitted '%s'", citation_change)
    else:
        logger.debug("Not emitted '%s'", citation_change)

@app.task(queue='maintenance')
def task_maintenance():
    """
    Maintenance operations
    """
    registered_records = db.get_registered_citation_targets(app)
    for registered_record in registered_records:
        bibcode = registered_record['bibcode']
        try:
            existing_citation_bibcodes = _get_citations(app, bibcode)
        except:
            logger.exception("Failed API request to retreive existing citations for bibcode '{}'".format(bibcode))
            continue
        dummy_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated
                                                       )
        parsed_metadata = db.get_citation_target_metadata(app, dummy_citation_change)['parsed']
        logger.debug("Calling 'task_output_results' with '%s'", dummy_citation_change)
        task_output_results.delay(dummy_citation_change, parsed_metadata, existing_citation_bibcodes)


@app.task(queue='output-results')
def task_output_results(citation_change, parsed_metadata, citations):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param citation_change: contains citation changes
    :return: no return
    """
    record, nonbib_record = forward.build_record(app, citation_change, parsed_metadata, citations)
    logger.debug('Will forward this record: %s', record)
    logger.debug("Calling 'app.forward_message' with '%s'", str(record))
    app.forward_message(record)
    logger.debug('Will forward this record: %s', nonbib_record)
    logger.debug("Calling 'app.forward_message' with '%s'", str(nonbib_record))
    app.forward_message(nonbib_record)


if __name__ == '__main__':
    app.start()
