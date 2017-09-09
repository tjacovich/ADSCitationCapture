from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
import adscc.app as app_module
#from adsmsg import CitationUpdate

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
        if citation_change.doi != "":
            # TODO: Fetch DOI metadata
            pass
        elif citation_change.pid != "":
            # TODO: Fetch ASCL metadata?
            pass
        elif citation_change.url != "":
            # TODO: Check is a valid and alive URL
            pass
        else:
            raise Exception("Citation change should have doi, pid or url informed: %s", citation_change)

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
