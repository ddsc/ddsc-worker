from __future__ import absolute_import

from celery import Celery

from ddsc_worker.logging import celeryconfig

celery = Celery('ddsc_worker.logging')
celery.config_from_object(celeryconfig)
