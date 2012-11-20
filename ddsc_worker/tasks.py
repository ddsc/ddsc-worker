from __future__ import absolute_import
from ddsc_worker.celery import celery
import time


@celery.task
def add(x, y):
    time.sleep(10)
    return x + y


@celery.task
def mul(x, y):
    time.sleep(2)
    return x * y
