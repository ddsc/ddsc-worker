'''
Created on Jan 28, 2013
@author: LuS
'''
from __future__ import absolute_import
import logging

from ddsc_worker.fugrotasks_forked import import_csv, data_delete
from ddsc_worker.import_auth import get_usr_by_ip

import sys
import string
import os

logger = logging.getLogger(__name__)

hdlr = logging.FileHandler('/home/shaoqing/ddsc.log')
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

def main():
    dstPathDir = "/home/shaoqing/dst"  # TO BE put in django settings
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    dst = dstPathDir + '/' + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)
    
    usr = get_usr_by_ip(fileName)
    
    if usr is False :
        data_delete(1, src)
        return
    
    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart","")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    #if auth_func(usr, sensorid):
    if fileExtension == ".csv":
            # csv_detected.delay(src)
        import_csv.delay(src, usr)
    else :
        file_ignored.delay(src, fileExtension)
    
def file_ignored(src, fileExtension):
    logger.info('''[x]--Warning-- * %r
    FILE: %r is not acceptable'''
    % (fileExtension, src))
    