from __future__ import absolute_import

from subprocess import call
import logging
import os
import shutil

from django.conf import settings
from django.contrib.auth.models import User
from pandas.io.parsers import read_csv
from tslib.readers import PiXmlReader

from ddsc_worker.import_auth import get_auth
from ddsc_worker.import_auth import get_remoteid_by_filename
from ddsc_worker.import_auth import get_timestamp_by_filename

logger = logging.getLogger(__name__)

pd = getattr(settings, 'IMPORTER_PATH')
ERROR_file = pd['storage_base_path'] + pd['rejected_file']
gs_setting = getattr(settings, 'IMPORTER_GEOSERVER')


def data_convert(src):
#    pydevd.settrace()
    try:
        logger.debug("[x] converting %r to pandas object" % (src))
        tsOBJ = read_csv(
            src, index_col=0,
            parse_dates=True,
            names=['SensorID', 'value'],
        )
        status = 1
    except:
        logger.error('CSV file: %r ERROR to convert!' % src)
        status = 0
    if status == 0:
        logger.error("[x] %r _FAILED to be converted" % (src))
        data_move(src, ERROR_file)
        raise Exception('CSV file: %r ERROR to convert!' % src)
        exit()
    else:
        tsOBJ['flag'] = 'None'
        tsOBJ = tsOBJ.sort()
        logger.debug("[x] %r _converted & sorted" % (src))
        return tsOBJ


def data_validate(tsOBJ, ts, src=None):
    # getting validation thresholds
    # did not handle the situation where the threshold could be "None"
    max_hard = ts.validate_max_hard
    min_hard = ts.validate_min_hard
    max_soft = ts.validate_max_soft
    min_soft = ts.validate_min_soft
    diff_hard = ts.validate_diff_hard
    diff_soft = ts.validate_diff_soft

    logger.debug("[x] validating %r" % (src))
    i = 0
    for row in tsOBJ.iterrows():
        if ((diff_hard < abs(tsOBJ.value[i - 1] - tsOBJ.value[i]))
        or (tsOBJ.value[i] > max_hard)
        or (tsOBJ.value[i] < min_hard)):
            tsOBJ['flag'][i] = '6'
            i += 1
        elif (diff_soft < abs(tsOBJ.value[i - 1] -
        tsOBJ.value[i]) or (tsOBJ.value[i] > max_soft)
         or (tsOBJ.value[i] < min_soft)):
            tsOBJ['flag'][i] = '3'
            i += 1
        else:
            tsOBJ['flag'][i] = '0'
            i += 1
    logger.debug("[x] %r _validated" % (src))
    return tsOBJ  # TO BE UPDATED AFTER THE REAL VALIDATION


def write2_cassandra(tsOBJ_yes, ts, src):
    # DONE: write to Cassandra and Postgres
    # DONE: follow Carsten's coming code
    try:
        tsOBJ_yes = tsOBJ_yes.tz_localize('UTC')
        del tsOBJ_yes['SensorID']
        ts.set_events(tsOBJ_yes)
        ts.save()
        logger.debug("[x] %r _written" % (src))
    except:
        logger.error("[x] %r _FAILED to be written to cassandra" % (src))
        data_move(src, ERROR_file)
        raise Exception('CSV file: %r ERROR to convert!' % src)
        exit()


def data_delete(wStatus, src):
    if wStatus == 1:
        os.remove(src)
        if 1 == 2:
            logger.error('something went wrong')
            pass
        else:
            logger.debug("[x] %r _deleted" % (src))
    else:
        pass


def import_csv(src, usr_id):
    usr = User.objects.get(id=usr_id)
    tsobj = data_convert(src)
    tsgrouped = tsobj.groupby('SensorID')
    nr = len(tsgrouped)
    nr = str(nr)
    logger.debug('There are %r timeseries in file : %r' % (nr, src))
    for name, tsobj_grouped in tsgrouped:
        remoteid = tsobj_grouped['SensorID'][0]
        ts = get_auth(usr, remoteid)  # user object and remote id
        if ts is False:
            data_move(src, ERROR_file)
            logger.error(
                '[x] File:--%r-- has been rejected because of authorization' %
                src)
            raise Exception("[x] %r _FAILED to be imported" % (src))
        else:
            tsobjYes = data_validate(tsobj_grouped, ts, src)
            st = write2_cassandra(tsobjYes, ts, src)

    data_delete(st, src)
    logger.info('[x] File:--%r-- has been successfully imported' % src)


def data_move(src, dst):
    if not os.path.exists(dst):
        os.makedirs(dst)
    try:
        shutil.move(src, dst)
        if os.path.isdir(dst):
            logger.debug("[x] Moved %r to %r" % (src, dst))
            return os.path.join(dst, os.path.split(src)[1])
        else:
            logger.debug("[x] Moved %r to %r" % (src, dst))
            return dst
    except EnvironmentError as e:
        logger.error(e)
        return


def import_file(src, filename, dst, usr_id):
    usr = User.objects.get(id=usr_id)
    logger.debug("[x] Importing %r to DB" % filename)
    timestamp = get_timestamp_by_filename(filename)

    remoteid = get_remoteid_by_filename(filename)
    ts = get_auth(usr, remoteid)

    if ts is not False:
        str_year = str(timestamp.year[0])
        str_month = str(timestamp.month[0])
        str_day = str(timestamp.day[0])
        store_dst = dst + ts.uuid + '/' + str_year +\
            '-' + str_month + '-' + str_day + '/'
        store_dstf = store_dst + filename
        values = {"value": store_dstf}

        ts.set_event(timestamp[0], values)
        ts.save()
        data_move(src + filename, store_dst)
        logger.info(
            '[x] File:--%r-- has been successfully imported' % (src +
            filename))
    else:
        logger.error('[x] File:--%r-- has been rejected' % (src + filename))
        data_move(src + filename, ERROR_file)
        raise Exception("[x] %r _FAILED to be imported" % (src + filename))


def import_geotiff(src, filename, dst, usr_id):
    usr = User.objects.get(id=usr_id)
    src = src + filename

    logger.debug("[x] Importing %r to DB" % filename)
    timestamp = get_timestamp_by_filename(filename)

    remoteid = get_remoteid_by_filename(filename)
    ts = get_auth(usr, remoteid)

    str_year = str(timestamp.year[0])
    str_month = str(timestamp.month[0])
    str_day = str(timestamp.day[0])
    store_dst = dst + ts.uuid + '/' +\
        str_year + '-' + str_month + '-' + str_day + '/'
    store_dstf = store_dst + filename

    logger.debug("[x] publishing %r into GeoServer..." % src)

    #  since it is a subprocess, we are not sure if it has been
    #  executed properly or not... Further check need to be done
    #  before proceed to the next stage
    values = {"value": store_dstf}

    if ts is not False:
        str_year = str(timestamp.year[0])
        str_month = str(timestamp.month[0])
        str_day = str(timestamp.day[0])
        store_dst = dst + ts.uuid + '/' +\
            str_year + '-' + str_month + '-' + str_day + '/'
        store_dstf = store_dst + filename

        hao = call(['java', '-jar',
            gs_setting['geoserver_jar_pusher'],
            src, gs_setting['geoserver_url'],
            gs_setting['geoserver_username'],
            gs_setting['geoserver_password']])

        if hao == 0:
            logger.debug("[x] Published %r to GeoServer " % src)
            ts.set_event(timestamp[0], values)
            ts.save()
            data_move(src, store_dst)
            logger.info(
                "[x] File:--%r-- has been successfully imported" % src)
        else:
            logger.debug("[x] Publishing error")
            logger.error('[x] File:--%r-- has been rejected' % src)
            data_move(src, ERROR_file)
            raise Exception("[x] %r _FAILED to be imported" % src)
    else:
        logger.error('[x] File:--%r-- has been rejected' % src)
        data_move(src, ERROR_file)
        raise Exception("[x] %r _FAILED to be imported" % src)


def import_pi_xml(src, usr_id):
    logger.info("[x] Importing %r" % src)
    reader = PiXmlReader(src)

    usr = User.objects.get(id=usr_id)

    for md, df in reader.get_series():
        loc = md['header']['locationId']
        para = md['header']['parameterId']
        unit = md['header']['timeStep']['@unit']
        try:
            div = md['header']['timeStep']['@divider']
        except:
            div = ''
        try:
            mul = md['header']['timeStep']['@multiplier']
        except:
            mul = ''
        code = loc + '::' + para + '::' + unit + '::' + div + '::' + mul
        ts = get_auth(usr, code)
        if ts is False:
            data_move(src, ERROR_file)
            logger.error(
                '[x] File:--%r-- has been rejected because of authorization' %
                src)
            raise Exception("[x] %r _FAILED to be imported" % (src))
        else:
            try:
                ts.set_events(df)
                ts.save()
                logger.debug("[x] %r _written" % (src))
            except:
                logger.error(
                    "[x] %r _FAILED to be written to cassandra" % (src))
                data_move(src, ERROR_file)
                raise Exception('piXML file: %r ERROR to convert!' % src)

    data_delete(1, src)
    logger.info('[x] File:--%r-- has been successfully imported' % src)


def file_ignored(src, fileExtension):
    logger.info('[x]--Warning-- * %r' % fileExtension + \
        ' FILE: %r is not acceptable' % src)
    dst = pd['storage_base_path'] + pd['unrecognized']
    data_move(src, dst)
    raise Exception("[x] %r _FAILED to be imported" % src)
