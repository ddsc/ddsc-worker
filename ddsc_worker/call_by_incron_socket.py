'''
Created on Jan 28, 2013
@author: LuS
'''
from __future__ import absolute_import
import logging

from ddsc_worker.importer import import_csv, data_move
from ddsc_worker.import_auth import get_usr_by_ip
from django.conf import settings
import sys
import string
import os


LOG_PATH = getattr(settings, 'LOGGING_DST')
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(LOG_PATH['ddsc_logging'])
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

DST_PATH = getattr(settings, 'IMPORTER')


def main():
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)

    usr = get_usr_by_ip(fileName)

    if usr is False:
        data_move(src, DST_PATH['rejected_csv'])
        return

    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart", "")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    #if auth_func(usr, sensorid):
    if fileExtension == ".csv":
        logger.info('[x] start importing: %r' % src)
        logger.info('By User: %r' % usr.username)
        import_csv.delay(src, usr)
    else:
        file_ignored(src, fileExtension)


def file_ignored(src, fileExtension):
    logger.info('[x]--Warning-- * %r\
    FILE: %r is not acceptable'\
    % (fileExtension, src))
