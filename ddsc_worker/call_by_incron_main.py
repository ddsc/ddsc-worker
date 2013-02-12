from __future__ import absolute_import
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from ddsc_worker.importer import import_csv
from ddsc_worker.importer import import_file
from ddsc_worker.importer import import_geotiff
from ddsc_worker.importer import data_move
from ddsc_worker.import_auth import get_usr_by_folder

from django.conf import settings

import sys
import os
import string

from ddsc_worker.logging.handlers import DDSCHandler
from ddsc_worker.celery import celery
import logging

DST_PATHS = getattr(settings, 'IMPORTER')


def setup_ddsc_task_logger(**kwargs):
    handler = DDSCHandler()
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"
    ))
    logger.addHandler(handler)

logger = get_task_logger(__name__)


def main():
    ## dummy HDFS folder
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName

    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)

    usr = get_usr_by_folder(pathDir)

    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart", "")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    #if auth_func(usr, sensorid):
    if fileExtension == ".csv":
            # csv_detected.delay(src)
        import_csv.delay(src, usr)
    elif (fileExtension == ".png") or \
    (fileExtension == ".jpg") or \
    fileExtension == ".jpeg":
        dst = DST_PATHS['image']
        import_file.delay(pathDir, fileName, dst, usr)
    elif fileExtension == ".avi" or \
    fileExtension == ".wmv":
        dst = DST_PATHS['video']
        import_file.delay(pathDir, fileName, dst, usr)
    elif fileExtension == ".pdf":
        dst = DST_PATHS['pdf']
        import_file.delay(pathDir, fileName, dst, usr)
    elif (fileExtension == ".tif" or \
    fileExtension == ".tiff"):
        dst = DST_PATHS['geotiff']
        import_geotiff.delay(pathDir, fileName, dst, usr)
    else:
        file_ignored.delay(src, fileExtension)


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
    dst = DST_PATHS['unrecognized']
    data_move(src, dst)
