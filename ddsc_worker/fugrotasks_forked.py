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


logger = logging.getLogger(__name__)

hdlr = logging.FileHandler('/home/shaoqing/ddsc.log')
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

ERROR_CSV = "/home/shaoqing/rejected_csv"

# @celery.task
def data_convert(src):
#    pydevd.settrace()
    try:
        logger.info("[x] converting %r to pandas object" % (src))
        tsOBJ = read_csv(src, index_col=0,
        parse_dates=True,
        names=['SensorID', 'Temperature'])
        status = 1
    except:
        print 'CSV file: ' + src + 'ERROR to convert!'
        status = 0
    if status == 0:
        logger.error("[x] %r _FAILED to be converted" % (src))
        data_move(src, ERROR_CSV)
        logger.info("[x] %r _MOVED to  %r" % (src, ERROR_CSV))
        exit()
    else:
        tsOBJ['Flag'] = 'None'
        # self.data_validate(tsOBJ)
        print "[x]  %r _converted" % (src)
        logger.info("[x] %r _converted" % (src))
        return tsOBJ


# @celery.task
def data_validate(tsOBJ, min_t=4, max_t=9, src=None):
    if 1 == 2:
        pass
    else:
        logger.info("[x] validating %r" % (src))
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
        print "[x] %r  _validated" % (src)
        logger.info("[x] %r _validated" % (src))

        print tsOBJ
        return tsOBJ  # TO BE UPDATED AFTER THE REAL VALIDATION


# @celery.task
def write2_cassandra(tsOBJ_yes, src):
    # DONE: write to Cassandra and Postgres
    # DONE: follow Carsten's coming code
    logger.info("[x] writing %r to Cassandra" % (src))
    series_id = '3201.WATHTE.onder.instantaneous'

    ts, _ = Timeseries.objects.get_or_create(code = series_id)
    # (ts, status) = result
    try :
        for timestamp, row  in tsOBJ_yes.iterrows():
            ts.set_event(timestamp, row)
        ts.save()
        wStatus = 1
        print ("[x] %r _written" % (src))
        logger.info("[x] %r _written" % (src))
    except :
        logger.error("[x] %r _FAILED to write to cassandra" % (src))
        data_move(src, ERROR_CSV)
        logger.info("[x] %r _MOVED to %r" % (src, ERROR_CSV))
        wStatus = 0
    return wStatus


# @celery.task
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


@celery.task
def import_csv(src, min_v, max_v):
    tsobj = data_convert(src)
    tsobjYes = data_validate(tsobj, min_v, max_v, src)
    st =  write2_cassandra(tsobjYes, src)    
    data_delete(st, src)


@celery.task(ignore_result=True)
def data_move(src, dst):
    print ("[x] Moving %r to %r" % (src, dst))
    logger.info("[x] Moving %r to %r" % (src, dst))
    try:
        shutil.move(src, dst)
        if os.path.isdir(dst):
            return os.path.join(dst, os.path.split(src)[1])
        else:
            return dst
    except EnvironmentError as e:
        logger.error(e)
        exit()


@celery.task(ignore_result=True)
def imagename_write_DB(src, dst):
    ## MAKE A DUMMY TIMESTAMP FOR PIC
    ## TODO: RETRIEVING FROM EXIF HEADER
    indx = pandas.date_range('2000-12-23T12:20:23Z', periods=1)
    df = DataFrame([dst], index=indx, columns=['Ref'])
    print ("[x] Writing %r to DB" % src)
    logger.info("[x] Writing %r to DB" % src)
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
    logger.info("[x] publishing %r into GeoServer..." % (src))
    hao = call(['java', '-jar',
        '/home/spd/localrepo/ddsc-worker/ddsc_worker/gs_push.jar',
        src, fileName])
    if hao == 0:
        print "[x] _published %r to GeoServer" % src
        logger.info("[x] Published %r to GeoServer " % src)
    else :
        print "[x] Publishing error"
        logger.error("[x] Publishing error")
    #  since it is a subprocess, we are not sure if it has been
    #  executed properly or not... Further check need to be done
    #  before proceed to the next stage


@celery.task
def import_lmw(src, fileName):
    date_spec = {"timedate": [0, 1]}
    tsOBJ = read_csv(src,
                     skiprows=6, parse_dates=date_spec,
                     sep=";", index_col=0, header=None)

    tsOBJ = tsOBJ.rename(columns
                         ={2:'location', 3:'met', 4:'q_flag', 5:'value'})

    f = open(src, 'r')

    str = f.readline()
    strlist = str.split('=')
    data_bank =  strlist[1].replace("\r\n",'')

    str = f.readline()
    strlist = str.split('=')
    location = strlist[1].replace("\r\n",'')

    str = f.readline()
    strlist = str.split('=')
    waarnemingsgroepcode = strlist[1].replace("\r\n",'')

    str = f.readline()
    strlist = str.split('=')
    x_cord = strlist[1].replace("\r\n",'')

    str = f.readline()
    strlist = str.split('=')
    y_cord = strlist[1].replace("\r\n",'')    

    i = 0
    tsOBJ['Flag'] = 'None'
    for row in tsOBJ.iterrows():
            if tsOBJ.q_flag[i] in [10, 30, 50, 70] :
                tsOBJ['Flag'][i] = '0'
                i += 1
            elif tsOBJ.q_flag[i] in [2, 22, 24, 28, 42, 44, 48, 62, 68]:
                tsOBJ['Flag'][i] = '3'
                i += 1
            else:
                tsOBJ['Flag'][i] = '6'
                i += 1
    print "[x] %r  _validated" % (src)
    logger.info("[x] %r _validated" % (src))
    tsOBJ = tsOBJ.tz_localize('UTC')
    del tsOBJ['location']
    del tsOBJ['met']
    del tsOBJ['q_flag']
    print tsOBJ

    st = write2_cassandra(tsOBJ, src)
    data_delete(st, src)
