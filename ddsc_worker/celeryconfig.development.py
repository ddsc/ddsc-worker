"""Celery development settings"""

BROKER_URL = "amqp://guest@rabbitmq:5672//"
CELERYD_LOG_LEVEL = "DEBUG"
CELERY_IMPORTS = ("ddsc_worker.tasks", )
