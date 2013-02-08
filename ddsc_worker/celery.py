from __future__ import absolute_import

from celery import Celery

from ddsc_worker import celeryconfig

celery = Celery('ddsc_worker')
celery.config_from_object(celeryconfig)
