from __future__ import absolute_import
from ddsc_worker.celery import celery


@celery.task
def add(x, y):
    return x + y


@celery.task
def mul(x, y):
    return x + y
