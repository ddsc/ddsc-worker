from __future__ import absolute_import
from celery import Celery

celery = Celery('ddsc_worker',
    broker='amqp://guest:guest@192.168.20.61:5672:/%2F',
    include=['ddsc_worker.tasks'])
