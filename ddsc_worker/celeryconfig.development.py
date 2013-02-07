"""Celery development settings"""

BROKER_URL = "amqp://guest@rabbitmq:5672//"
CELERY_IMPORTS = ("ddsc_worker.tasks", )

CELERY_TASK_SERIALIZER = "json"
