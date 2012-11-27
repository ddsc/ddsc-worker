from __future__ import absolute_import
from ddsc_worker import celeryconfig
from kombu.common import Broadcast

try:
    BROKER_URL = celeryconfig.BROKER_URL
except ImportError:
    pass

CELERY_IMPORTS = ("ddsc_worker.logging.tasks", )
CELERY_QUEUES = (Broadcast("ddsc.log"), )
CELERY_TASK_SERIALIZER = "json"
