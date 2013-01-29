from __future__ import absolute_import
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from ddsc_worker.fugrotasks_forked import import_csv
from ddsc_worker.fugrotasks_forked import import_file
from ddsc_worker.fugrotasks_forked import import_geotiff
from ddsc_worker.import_auth import get_usr_by_folder

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


def main():
    ## dummy HDFS folder
    dstPathDir = "/home/shaoqing/dst"  # TO BE put in django settings
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    dst = dstPathDir + '/' + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)
    
    usr = get_usr_by_folder(pathDir)

    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart","")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    #if auth_func(usr, sensorid):
    if fileExtension == ".csv":
            # csv_detected.delay(src)
        import_csv.delay(src, usr)
    elif (fileExtension == ".png"
    or fileExtension == ".jpg"
    or fileExtension == ".jpeg"
    or fileExtension == ".avi"
    or fileExtension == ".wmv"
    or fileExtension == ".pdf"):
        import_file.delay(pathDir, fileName, usr)
    elif (fileExtension == ".tif"
    or fileExtension == ".tiff"):
        import_geotiff.delay(src, usr)
    else:
        file_ignored.delay(src, fileExtension)
 #   else:
 #       auth_failed(usr, fileName)


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

