#from ddsc_core.models import DataStore
from __future__ import absolute_import
from celery.utils.log import get_task_logger
from ddsc_worker.celery import celery
from tslib.readers import PiXmlReader
import gzip
import os
import shutil
import time

logger = get_task_logger(__name__)


@celery.task
def add(x, y):
    time.sleep(9)
    return x + y


@celery.task
def mul(x, y):
    time.sleep(3)
    return x * y


@celery.task(ignore_result=True)
def import_pi_xml(src):
    logger.info("Importing %r" % src)
    reader = PiXmlReader(src)
    for md, df in reader.get_series():
        # TODO: store metadata in postgress
        # TODO: store dataframe in cassandra
        pass
    return src


@celery.task(ignore_result=True)
def move(src, dst):
    logger.info("Moving %r to %r" % (src, dst))
    shutil.move(src, dst)
    if os.path.isdir(dst):
        return os.path.join(dst, os.path.split(src)[1])
    else:
        return dst


@celery.task(ignore_result=True)
def compress(src):
    logger.info("Gzipping %r" % src)
    dst = src + ".gz"
    with open(src, "rb") as f_in:
        with gzip.open(dst, "wb") as f_out:
            f_out.writelines(f_in)
    os.remove(src)
    return dst
