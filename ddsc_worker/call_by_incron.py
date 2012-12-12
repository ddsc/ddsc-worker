from __future__ import absolute_import
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from ddsc_worker.fugrotasks import data_convert
from ddsc_worker.fugrotasks import data_validate
from ddsc_worker.fugrotasks import write2_cassandra
from ddsc_worker.fugrotasks import data_delete
from ddsc_worker.fugrotasks import data_move
from ddsc_worker.fugrotasks import imagename_write_DB
from ddsc_worker.fugrotasks import publish_gserver

import sys
import os
import string

from ddsc_worker.logging.handlers import DDSCHandler
from ddsc_worker.celery import celery
import logging


@after_setup_task_logger.connect
def setup_ddsc_task_logger(**kwargs):
    handler = DDSCHandler()
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"
    ))
    logger.addHandler(handler)

logger = get_task_logger(__name__)


@celery.task
def main():
    ## dummy HDFS folder
    dstPathDir = "/home/spd/testdata/dst"
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    dst = dstPathDir + '/' + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)

    usr = get_usr()
    sensorid = get_sensor_id(fileName)

    if auth_func(usr, sensorid):
        if fileExtension == ".csv":
            csv_detected.delay(src)
            (data_convert.s(src) |
            data_validate.s(4, 7, src) |
            write2_cassandra.s(src) |
            data_delete.s(src))()
        elif (fileExtension == ".png"
            or fileExtension == ".jpg"
            or fileExtension == ".jpeg"):
                img_detected.delay(src)
                (imagename_write_DB.si(src, dst) |
                data_move.si(src, dstPathDir))()
        elif (fileExtension == ".avi"
            or fileExtension == ".wmv"
            or fileExtension == ".pdf"
            or fileExtension == ".netcdf"):
            mul_media_detected.delay(src)
            (imagename_write_DB.si(src, dst) |
            data_move.si(src, dstPathDir))()  # for
            # the time being is the same to image
        elif (fileExtension == ".tif"
            or fileExtension == ".tiff"):
            ## TODO: Add Auth
            rs_data_detected.delay(src)
            (imagename_write_DB.si(src, dst) |
            publish_gserver.si(src, fileName) |
            data_move.si(src, dstPathDir))()
        else:
            file_ignored.delay(src, fileExtension)
    else:
        auth_failed(usr, fileName)


@celery.task
def csv_detected(src):
    logger.info('[x]--CSV-- %r detected' % src)


@celery.task
def img_detected(src):
    logger.info('[x]--Image-- %r detected' % src)


@celery.task
def mul_media_detected(src):
    logger.info('[x]--Multimedia-- %r detected' % src)


@celery.task
def rs_data_detected(src):
    logger.info('[x]--Remote Sensing Data-- %r detected' % src)


@celery.task
def file_ignored(src, fileExtension):
    logger.info('''[x]--Warning-- * %r
    FILE: %r is not acceptable'''
    % (fileExtension, src))


def auth_func(usr, sensor_id):
    #  TODO: 
    #  Check database
    #  Some hash matching
    #  Verify 
    return True  # Currently, always return true


@celery.task
def auth_failed(usr, fileName):
    logger.info('%r is not allowed to upload file: %r' % (usr, fileName))


def get_usr():
    #  TODO:
    #  get user name from sftp login info
    usr = 'speeder'
    return usr


def get_sensor_id(fileName):
    #  TODO:
    #  get sensor_id from fileName
    #  TODO: intercept sensor_id from fileName
    snrid = 'dummy_speeder_sensor_001'
    return snrid
