from __future__ import absolute_import
import celery


@celery.task(exchange="ddsc.log", ignore_result=True)
def log(msg):
    print msg
