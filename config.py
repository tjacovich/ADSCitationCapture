#LOGGING_LEVEL = 'WARN'
#LOGGING_LEVEL = 'DEBUG'
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True

CELERY_INCLUDE = 'ADSCitationCapture.tasks'
CELERY_BROKER = 'pyamqp://user:password@localhost:5672/citation_capture_pipeline'
OUTPUT_CELERY_BROKER = 'pyamqp://user:password@localhost:5672/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'

SQLALCHEMY_URL = 'postgres://postgres@localhost:5432/citation_capture_pipeline'
SQLALCHEMY_ECHO = False

ADS_WEBHOOK_URL = "http://adsabs.harvard.edu/webhooks/trigger"
ADS_WEBHOOK_AUTH_TOKEN = "This is a secret!"
DOI_URL = "https://doi.org/"
DATACITE_URL = "https://api.datacite.org/works/"
ASCL_URL = "http://ascl.net/"

# When 'True', it converts all the asynchronous calls into synchronous,
# thus no need for rabbitmq and allow debuggers to run if needed:
CELERY_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
