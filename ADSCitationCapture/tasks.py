from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
import ADSCitationCapture.app as app_module
import ADSCitationCapture.webhook as webhook
import ADSCitationCapture.doi as doi
import ADSCitationCapture.url as url
import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCitationCaptureCelery('ads-citation-capture', proj_home=proj_home)
logger = app.logger


app.conf.CELERY_QUEUES = (
    Queue('process-citation-changes', app.exchange, routing_key='process-citation-changes'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)


# ============================= TASKS ============================================= #

@app.task(queue='process-citation-changes')
def task_process_citation_changes(citation_changes):
    """
    Process citation changes
    """
    logger.debug('Checking content: %s', citation_changes)
    for citation_change in citation_changes.changes:
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi \
            and citation_change.content not in ["", None]:
            # Fetch DOI metadata (if HTTP request fails, an exception is raised
            # and the task will be re-queued (see app.py and adsputils))
            is_software = doi.is_software(app.conf['DOI_URL'], citation_change.content)
            is_link_alive = True
        elif citation_change.content_type == adsmsg.CitationChangeContentType.pid \
            and citation_change.content not in ["", None]:
            is_software = True
            is_link_alive = url.is_alive(app.conf['ASCL_URL'] + citation_change.content)
        elif citation_change.content_type == adsmsg.CitationChangeContentType.url \
            and citation_change.content not in ["", None]:
            is_software = False
            is_link_alive = url.is_alive(citation_change.content)
        else:
            is_software = False
            is_link_alive = False
            logger.error("Citation change should have doi, pid or url informed: {}", citation_change)
            #raise Exception("Citation change should have doi, pid or url informed: {}".format(citation_change))

        emitted = False
        if is_software and is_link_alive:
            emitted = webhook.emit_event(app.conf['ADS_WEBHOOK_URL'], app.conf['ADS_WEBHOOK_AUTH_TOKEN'], citation_change)

        if emitted:
            logger.debug("Emitted '%s'", citation_change)
        else:
            logger.debug("Not emitted '%s'", citation_change)

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
