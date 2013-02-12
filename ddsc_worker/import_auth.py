"""
the authorization for ddsc worker
"""
from __future__ import absolute_import

import logging

from ddsc_core.models.models import Timeseries, IdMapping
from ddsc_core.models.system import IPAddress, Folder
from ddsc_core.auth import PERMISSION_CHANGE
from django.contrib.auth.models import User
from django.conf import settings

import pandas

LOGGING_PATH = getattr(settings, 'LOGGING_DST')

logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(LOGGING_PATH['ddsc_logging'])
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def get_auth(usr, remote_id):
    #  TODO: put the commented back after fixing has_perm problem
    ts = get_timeseries_by_remoteid(usr, remote_id)
    if usr.has_perm(PERMISSION_CHANGE, ts):
        logger.info(
            'user: %r has permission to timeseries: %r' %
            (usr.username, ts.name)
        )
        return ts
    else:
        logger.error(
            'user: %r has NO permission to timeseries: %r' %
            (usr.username, ts.name)
        )
        return False


def get_usr_by_ip(fileName):
    #  TODO:
    #  the seperator shall be _ like 192.168.22.130_bla001.csv
    f = fileName.find("_")
    iplable = fileName[0:f]
    try:
        ip = IPAddress.objects.get(label=iplable)
        usr = ip.user
        return usr
    except IPAddress.DoesNotExist:
        logger.error("Ip address: %r does not exist in data base" % iplable)
        return False


def get_usr_by_folder(pathDir):
    usr_name = pathDir.replace("/home/", "")
    f = usr_name.find("/")
    usr_name = usr_name[0:f]
    try:
        folder = Folder.objects.get(path=pathDir)
    except Folder.DoesNotExist:
        logger.error("Path Name : %r does not exist in data base" % pathDir)
        return 0
    try:
        usr = User.objects.get(id=folder.user_id)
        return usr
    except User.DoesNotExist:
        logger.error("User: %r does not exist in data base" % usr_name)
        return 0


def get_timeseries_by_remoteid(usr, remoteid):
    #  TODO:
    #  get sensor_id from fileName
    #  TODO: intercept sensor_id from fileName
    try:
        idmp = IdMapping.objects.get(user=usr, remote_id=remoteid)
        ts = idmp.timeseries
        return ts
    except IdMapping.DoesNotExist:
        logger.info(
            "No such timeseries remoteID : %r---%r in database\
            or it is already an internal id"
            % (usr.username, remoteid)
        )
        try:
            ts = Timeseries.objects.get(uuid=remoteid)
            logger.info('It is already an internal id')
            return ts
        except Timeseries.DoesNotExist:
            logger.error("sorry, %r is not an internal id neither" % remoteid)
            return 0


def get_remoteid_by_filename(filename):
    f = filename.find("_")
    remoteid = filename[0:f]
    return remoteid


def get_timestamp_by_filename(filename):
    f1 = filename.find("_")
    f2 = filename.find(".")
    tmstmp = filename[f1 + 1:f2]
    timestamp = pandas.date_range(tmstmp, periods=1)
    return timestamp
