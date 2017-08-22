
CELERY_BROKER = 'pyamqp://user:password@localhost:5672/citation_capture_pipeline'
OUTPUT_CELERY_BROKER = 'pyamqp://user:password@localhost:5672/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'

SQLALCHEMY_URL = 'postgres://user:password@localhost:5432/docker'
SQLALCHEMY_ECHO = False

