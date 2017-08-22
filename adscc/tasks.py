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
    Queue('check-citation', app.exchange, routing_key='check-if-extract'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)


# ============================= TASKS ============================================= #

@app.task(queue='check-citation')
def task_check_citation(message):
    """
    Checks if the citation needs to be processed
    """
    logger.debug('Checking content: %s', message)

@app.task(queue='output-results')
def task_output_results(msg):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param msg: contains the bibliographic metadata

            {'bibcode': '....',
             'authors': [....],
             'title': '.....',
             .....
            }
    :return: no return
    """
    logger.debug('Will forward this record: %s', msg)
    #rec = CitationUpdate(**msg)
    rec = None
    logger.debug("Calling 'app.forward_message' with '%s'", str(rec))
    app.forward_message(rec)


if __name__ == '__main__':
    app.start()
