from __future__ import absolute_import

from kombu.common import Broadcast

from ddsc_worker import celeryconfig

try:
    BROKER_URL = celeryconfig.BROKER_URL
except ImportError:
    pass

CELERYD_CONCURRENCY = 1
CELERY_IMPORTS = ("ddsc_worker.logging.tasks", )
CELERY_QUEUES = (Broadcast("ddsc.log"), )
CELERY_TASK_SERIALIZER = "json"
