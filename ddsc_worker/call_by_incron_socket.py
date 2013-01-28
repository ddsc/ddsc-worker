'''
Created on Jan 28, 2013
@author: LuS
'''
from __future__ import absolute_import
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from ddsc_worker.fugrotasks_forked import import_csv
from ddsc_worker.import_auth import get_usr_by_ip

import sys
import string
import os

def main():
    dstPathDir = "/home/shaoqing/dst"  # TO BE put in django settings
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    dst = dstPathDir + '/' + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)
    
    usr = get_usr_by_ip(fileName)
    
    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart","")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    #if auth_func(usr, sensorid):
    if fileExtension == ".csv":
            # csv_detected.delay(src)
#        import_csv.delay(src, usr)
        print 'this user is:' + usr.username
    else :
        file_ignored.delay(src, fileExtension)
    
def file_ignored(src, fileExtension):
    logger.info('''[x]--Warning-- * %r
    FILE: %r is not acceptable'''
    % (fileExtension, src))
    