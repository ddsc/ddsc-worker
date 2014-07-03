# (c) Fugro Geoservices. MIT licensed, see LICENSE.rst.
from __future__ import absolute_import

from uuid import UUID
import logging

from django.contrib.auth.models import User
import pandas

from ddsc_core.auth import PERMISSION_CHANGE
from ddsc_core.models import Folder
from ddsc_core.models import IPAddress
from ddsc_core.models import IdMapping
from ddsc_core.models import Timeseries

logger = logging.getLogger(__name__)


def get_auth(usr, remote_id):
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


def can_change(user, timeseries):
    """Can user change timeseries?

    Parameters
    ----------
    user: an instance of django.contrib.auth.models.User.
    timeseries: an instance of ddsc_core.models.Timeseries.

    Returns
    -------
    True or False (bool).

    """
    return user.has_perm(PERMISSION_CHANGE, timeseries)


def get_usr_by_ip(fileName):
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
        usr = User.objects.get(pk=folder.user_id)
        return usr
    except User.DoesNotExist:
        logger.error("User does not exist in data base")
        return 0


def get_timeseries_by_remoteid(usr, remoteid):
    try:
        idmp = IdMapping.objects.get(user=usr, remote_id=str(remoteid))
        ts = idmp.timeseries
        return ts
    except IdMapping.DoesNotExist:
        logger.debug(
            'No such timeseries remoteID : %r--' % usr.username +
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

    # Tricky code? A filename is expected to have 2 underscores at most?
    # Wouldn't a regular expression be more appropriate?

    f1 = filename.find("_")

    if filename.count("_") > 1:
        f2 = filename.find("_", f1 + 1)
    else:
        f2 = filename.find(".", f1 + 1)

    tmstmp = filename[f1 + 1:f2]

    # Pandas does not parse the UTC timezone designator 'Z' correctly.
    # This is solved by setting tz explicitly. No timezone designator
    # is implictly regarded as UTC. Other timezone designators are
    # not (yet) supported.

    if tmstmp.endswith('Z'):
        tmstmp = tmstmp[:-1]

    timestamp = pandas.date_range(tmstmp, periods=1, tz='UTC')

    return timestamp


def get_timeseries_by_filename(filename, user):
    """Return the timeseries corresponding to filename.

    Uploaded files may either start with a remote id or a UUID.
    Assumption: remote ids must not contain any underscores.

    Parameters
    ----------
    filename (str): a file name.
    user (django.contrib.auth.models.User): the person that uploaded this file.

    Returns
    -------
    A ddsc_core.models.Timeseries object (or None if there is no match).

    """
    ID = filename.split('_')[0]

    # A remote id can be anything, including a UUID,
    # so this check should be done first:

    try:
        obj = IdMapping.objects.get(user=user, remote_id=ID)
        return obj.timeseries
    except IdMapping.DoesNotExist:
        pass

    # Check if a DDSC UUID was used:

    try:
        uuid = UUID(ID)
        return Timeseries.objects.get(uuid=uuid)
    except (ValueError, Timeseries.DoesNotExist):
        pass

    logger.error(
        "Cannot map %s onto a timeseries for user %s.", filename, user)
