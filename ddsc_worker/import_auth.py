"""
the authorization for ddsc worker
"""
from __future__ import absolute_import

import logging

from ddsc_core.models.models import Timeseries, IdMapping
from ddsc_core.models.system import IPAddress, Folder
from ddsc_core.auth import PERMISSION_CHANGE
from django.contrib.auth.models import User

from django.contrib.auth.models import Group

import pandas as pd

logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('/home/shaoqing/ddsc.log')
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

def get_auth(usr, remote_id):
    #  TODO: put the commented back after fixing has_perm problem
    ts = get_timeseries_by_remoteid(usr, remote_id)
    logger.warning('Now, permission is always granted since usr.has_perm misbehave!')
#    if usr.has_perm(PERMISSION_CHANGE, ts) :
#        print 'should print out'
#        return ts
#    else :
#        return False
    return ts

def get_usr_by_ip(fileName):
    #  TODO:
    #  the seperator shall be _ like 192.168.22.130_bla001.csv
    f = fileName.find("_")
    iplable = fileName[0:f]
    try :
        ip = IPAddress.objects.get(label=iplable)
        usr = ip.user
        return usr
    except IPAddress.DoesNotExist :
        logger.error("Ip address: %r does not exist in data base" % iplable)
        return False
        
        
def get_usr_by_folder(pathDir):
    usr_name = pathDir.replace("/home/","") 
    f = usr_name.find("/")
    usr_name = usr_name[0:f]
    try :
        folder = Folder.objects.get(path=pathDir)
    except Folder.DoesNotExist :
        logger.error("Path Name : %r does not exist in data base" % pathDir)
    try :
        usr = User.objects.get(id=folder.user_id)
        return usr
    except User.DoesNotExist :
        logger.error("User: %r does not exist in data base" % usr_name)
        return 0
    
    
def get_timeseries_by_remoteid(usr, remoteid):
    #  TODO:
    #  get sensor_id from fileName
    #  TODO: intercept sensor_id from fileName
    try :
        idmp = IdMapping.objects.get(user=usr, remote_id=remoteid)
        ts = idmp.timeseries
        return ts
    except IdMapping.DoesNotExist :
        logger.info("No such timeseries remoteID : %r---%r in database or it is already an interanl id" % (usr.username, remoteid))
        try : 
            ts = Timeseries.objects.get(code=remoteid)
            return ts
        except Timeseries.DoesNotExist :
            logger.error("No permission to write timeseries with remoteID: %r by: %r" %(remoteid, usr.username))
            return 0
        
def get_remoteid_by_filename(filename): # file name should be: <timeseries_id>_<datetime>.*
    f = filename.find("_")
    remoteid = filename[0:f]
    return remoteid

def get_timestamp_by_filename(filename): # file name should be: <timeseries_id>_<datetime>.*
    f1 = filename.find("_")
    f2 = filename.find(".")
    tmstmp = filename[f1+1:f2]
    timestamp = pd.date_range(tmstmp, periods=1)
    return timestamp

#def get_ts_perm(usr, ts):
#    permission_gp = Group.objects.get(name = 'timeseries')
#    
#    data_ower = ts.owner
#    data_set = data_owner.data_set
#    
#    if ts.owner_id = balabala:
#        pass
    

