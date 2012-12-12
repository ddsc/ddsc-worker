from __future__ import absolute_import
from celery.utils.log import get_task_logger
from celery.signals import after_setup_task_logger
from ddsc_worker.logging.handlers import DDSCHandler
import logging

from ddsc_worker.celery import celery

from pandas.io.parsers import read_csv
from pandas import DataFrame
import pandas
# import pydevd # for remote debugging propose
from ddsc_core.models import Timeseries

from os import remove
import os
import shutil
from subprocess import call


@after_setup_task_logger.connect
def setup_ddsc_task_logger(**kwargs):
    handler = DDSCHandler()
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"))
    logger.addHandler(handler)


logger = get_task_logger(__name__)


@celery.task
def data_convert(src):
#    pydevd.settrace()
    try:
        tsOBJ = read_csv(src, index_col=0,
        parse_dates=True,
        names=['Temperature', 'SensorID'])
        status = 1
    except:
        print 'CSV file: ' + src + 'ERROR to convert!'
        status = 0
    if status == 0:
        pass
    else:
        tsOBJ['Flag'] = 'None'
        # self.data_validate(tsOBJ)
        print "[x]  %r _converted" % (src)
        logger.info("[x] %r _converted" % (src))
        return tsOBJ


@celery.task
def data_validate(tsOBJ, min_t=4, max_t=9, src=None):
    if 1 == 2:
        pass
    else:
        i = 0
        for row in tsOBJ.iterrows():
            if max_t < abs((tsOBJ.Temperature[i - 1]) -
            (tsOBJ.Temperature[i])):
                tsOBJ['Flag'][i] = '6'
                i += 1
            elif min_t < abs((tsOBJ.Temperature[i - 1]) -
            (tsOBJ.Temperature[i])):
                tsOBJ['Flag'][i] = '3'
                i += 1
            else:
                tsOBJ['Flag'][i] = '0'
                i += 1
        print "[x] _validated"
        logger.info("[x] %r _validated" % (src))
        return tsOBJ  # TO BE UPDATED AFTER THE REAL VALIDATION


@celery.task
def write2_cassandra(tsOBJ_yes, src):
    # DONE: write to Cassandra and Postgres
    # DONE: follow Carsten's coming code
    series_id = 'DummySeriesID'
    ts, _ = Timeseries.objects.get_or_create(code=series_id)
    # (ts, status) = result
    for timestamp, row  in tsOBJ_yes.iterrows():
        ts.set_event(timestamp, row)
        ts.save()
    wStatus = 1
    logger.info("[x] %r _written" % (src))
    return wStatus


@celery.task
def data_delete(wStatus, src):
    if wStatus == 1:
        remove(src)
        if 1 == 2:
        #            TODO
        #            ERROR Handeling
            pass
        else:
            print " [x]  %r _deleted" % (src)
            logger.info("[x] %r _deleted" % (src))
    else:
        pass


@celery.task(ignore_result=True)
def data_move(src, dst):
    logger.info("[x] Moving %r to %r" % (src, dst))
    shutil.move(src, dst)
    if os.path.isdir(dst):
        return os.path.join(dst, os.path.split(src)[1])
    else:
        return dst


@celery.task(ignore_result=True)
def imagename_write_DB(src, dst):
    ## MAKE A DUMMY TIMESTAMP FOR PIC
    ## TODO: RETRIEVING FROM EXIF HEADER
    indx = pandas.date_range('2000-12-23T12:20:23Z', periods=1)
    df = DataFrame([dst], index=indx, columns=['Ref'])
    logger.info("[x] Writing %r to DB " % src)
    ts, _ = Timeseries.objects.get_or_create(code="dummyImageSeriesID")
    for timestamp, row in df.iterrows():
        ts.set_event(timestamp, row)
        ts.save()


@celery.task(ignore_result=True)
def publish_gserver(src, fileName):
    #  call java .jar
    #  we still dont have the control of specify the
    #  workspace, storage, or layer name... TOBE added
    #  create Geoserver data storage name according to fileName
    call(['java', '-jar',
        '/home/spd/localRepo/ddsc-worker/ddsc_worker/gs_publish.jar',
        src, fileName])
    logger.info("[x] Publishing %r to GeoServer " % src)
    #  since it is a subprocess, we are not sure if it has been
    #  executed properly or not... Further check need to be done
    #  before proceed to the next stage
