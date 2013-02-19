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

pd = getattr(settings, 'IMPORTER_PATH')

logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(pd['ddsc_logging'])
formatter = logging.Formatter("[%(asctime)s: %(levelname)s/] %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def get_auth(usr, remote_id):
    #  TODO: put the commented back after fixing has_perm problem
    ts = get_timeseries_by_remoteid(usr, remote_id)
    if ts == 0:
        return False
    if usr.has_perm(PERMISSION_CHANGE, ts):
        logger.debug(
            'user: %r has permission to timeseries: %r' %
            (usr.username, ts.uuid)
        )
        return ts
    else:
        logger.error(
            'user: %r has NO permission to timeseries: %r' %
            (usr.username, ts.uuid)
        )
        return False


def get_usr_by_ip(fileName):
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
    try:
        folder = Folder.objects.get(path=pathDir)
    except Folder.DoesNotExist:
        logger.error("Path Name : %r does not exist in data base" % pathDir)
        return 0
    try:
        usr = User.objects.get(id=folder.user_id)
        return usr
    except User.DoesNotExist:
        logger.error("User does not exist in data base")
        return 0


def get_timeseries_by_remoteid(usr, remoteid):
    try:
        idmp = IdMapping.objects.get(user=usr, remote_id=remoteid)
        ts = idmp.timeseries
        return ts
    except IdMapping.DoesNotExist:
        logger.debug(
            'No such timeseries remoteID : %r--' % usr.username + \
            '%r in database or it is already an internal id' % remoteid
        )
        try:
            ts = Timeseries.objects.get(uuid=remoteid)
            logger.debug('It is already an internal id')
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
