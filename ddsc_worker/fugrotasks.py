from __future__ import absolute_import
from celery.utils.log import get_task_logger
from ddsc_worker.celery import celery

from pandas.io.parsers import read_csv
# from ddsc_core.models import timeseries
# from cassandralib.model import CassandraDataStore
# import pydevd # for remote debugging propose

import os
from os import remove
import shutil
import time

logger = get_task_logger(__name__)

@celery.task
def data_convert(src):
#    pydevd.settrace()
    try:
        tsOBJ=read_csv(src, index_col=0, parse_dates = True, names=['Temperature','SensorID'])
        status = 1
    except:
        print 'CSV file: ' + src + 'ERROR to convert!'
        status = 0
    if status==0:
        msg = src + "_convert"    # massage contains file name and failure typ
    else: 
        tsOBJ['Flag'] = 'None'
        msg = "yes"
        # self.data_validate(tsOBJ)
        print "[x]  %r _converted" % (src)
    	return tsOBJ

@celery.task
def data_validate(tsOBJ, min_t=4, max_t=9):
    if 1==2:
        pass
#        msg = self.fileDirName + "_validate"    # massage contains file name and failure type
    else:
        i=0
        for row in tsOBJ.iterrows():
            if max_t<abs((tsOBJ.Temperature[i-1])-(tsOBJ.Temperature[i])):
                tsOBJ['Flag'][i] = '6'
                i+=1
            elif min_t<abs((tsOBJ.Temperature[i-1])-(tsOBJ.Temperature[i])):
                tsOBJ['Flag'][i] = '3'
                i+=1
            else:
                tsOBJ['Flag'][i] = '0'
                i+=1
        print "[x] _validated"
	return tsOBJ # TO BE UPDATED AFTER THE REAL VALIDATION

@celery.task
def write2_cassandra(tsOBJ_yes):      
    # TODO: write to Cassandra and Postgres
    # TODO: follow Carsten's coming code
    a = 2 + 2
    wStatus = 1
    print "[x] _written"
    return wStatus

@celery.task
def data_delete(wStatus,src):      
    if wStatus == 1:
        remove(src)
	if 1==2:
	#            TODO
	#            ERROR Handeling
	    pass
	else: 
            print " [x]  %r _deleted" % (src)
    else:
        pass


