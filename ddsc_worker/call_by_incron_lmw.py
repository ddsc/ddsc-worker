from __future__ import absolute_import

from ddsc_worker.fugrotasks_forked import import_lmw
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

# from ddsc_worker.fugrotasks import data_convert
# from ddsc_worker.fugrotasks import data_validate
# from ddsc_worker.fugrotasks import write2_cassandra
# from ddsc_worker.fugrotasks import data_delete
from ddsc_worker.fugrotasks_forked import import_csv
from ddsc_worker.fugrotasks_forked import data_move
from ddsc_worker.fugrotasks_forked import imagename_write_DB
from ddsc_worker.fugrotasks_forked import publish_gserver

import sys
import os
import string

from ddsc_worker.logging.handlers import DDSCHandler
from ddsc_worker.celery import celery
import logging

from django.core.management.base import BaseCommand
from ddsc_core.models import IPAddress, Timeseries
from ddsc_core.auth import PERMISSION_CHANGE



@celery.task
def main():
    pathDir = sys.argv[1] + "/"
    fileName = sys.argv[2]
    src = pathDir + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)

    ## To implement authorization
    ## 11.01.2013
    ip_lable = "127.0.0.1"
    series_code = "3201.WATHTE.onder.instantaneous"        
    sensorid = series_code
#    usr = get_usr(ip_lable)
#    print usr
 #   sensorid = get_sensor_id(series_code)
#    usr = "spd"
#    sensorid = "001fugro"

#    tt = auth_func(usr, sensorid)
#    print tt
        
    if fileExtension == ".filepart":
        # TODO
	fileName = fileName.replace(".filepart","")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    if fileExtension == ".csv":
        import_lmw.delay(src,fileName)
    else:
        print "not a csv file"
