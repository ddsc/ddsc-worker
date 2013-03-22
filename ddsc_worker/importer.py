from __future__ import absolute_import

from subprocess import call
import logging
import os
import shutil

from django.conf import settings
from django.contrib.auth.models import User
from pandas.io.parsers import read_csv
from pandas import DataFrame
from tslib.readers import PiXmlReader
from datetime import datetime, timedelta

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
    usr = User.objects.get(pk=usr_id)
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
            write2_cassandra(tsobjYes, ts, src)

    data_delete(1, src)
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
    usr = User.objects.get(pk=usr_id)
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
    usr = User.objects.get(pk=usr_id)
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

    usr = User.objects.get(pk=usr_id)

    for md, df in reader.get_series():
        loc = md['header']['locationId']  # required
        para = md['header']['parameterId']  # required
        unit = md['header']['timeStep']['@unit']  # required
        div = md['header']['timeStep'].get('@divider', '')  # optional
        mul = md['header']['timeStep'].get('@multiplier', '')  # optional
        remote_id = loc + '::' + para + '::' + unit + '::' + div + '::' + mul
        ts = get_auth(usr, remote_id)
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
    logger.info('[x]--Warning-- * %r' % fileExtension +
        ' FILE: %r is not acceptable' % src)
    dst = pd['storage_base_path'] + pd['unrecognized']
    data_move(src, dst)
    raise Exception("[x] %r _FAILED to be imported" % src)


def import_lmw(DestinationPath, admFileName, datFileName, kwaFileName):
    ## get the user which in this case should be LMW I guess
    usr = User.objects.get(username='lmw_ddsc')
    adm_src = admFileName
    dat_src = datFileName
    kwa_src = kwaFileName

    tsobj_indx = ReadLMW(adm_src, dat_src, kwa_src)
    tsgrouped = tsobj_indx.groupby('SensorID')
    nr = len(tsgrouped)
    nr = str(nr)
    logger.debug('There are %r timeseries in file : %r' % (nr, adm_src))
    for name, tsobj_grouped in tsgrouped:
        remoteid = tsobj_grouped['SensorID'][0]
        ts = get_auth(usr, remoteid)  # user object and remote id
        if ts is False:
            data_move(adm_src, ERROR_file)
            data_move(dat_src, ERROR_file)
            data_move(kwa_src, ERROR_file)
            logger.error(
                '[x] File:--%r-- has been rejected because of authorization' %
                adm_src)
            raise Exception("[x] %r _FAILED to be imported" % (adm_src))
        else:
            tsobjYes = tsobj_grouped
            write2_cassandra(tsobjYes, ts, dat_src)

    data_delete(1, adm_src)
    data_delete(1, dat_src)
    data_delete(1, kwa_src)
    logger.info('[x] File:--%r-- has been successfully imported' % adm_src)


def ReadLMW(admFile, datFile, kwaFile):
    with open(admFile) as f:
        administration = f.readlines()
    with open(datFile) as f:
        data = f.readlines()
    with open(kwaFile) as f:
        data_quality = f.readlines()

    if len(administration) != len(data):
        raise Exception("Input data is not of same length.")

    # LMW interval in minutes
    interval = 10
    val_series = []
    timestamp_series = []
    remoteid_series = []
    quality_series = []

    for i in range(len(administration)):
        values = administration[i].split(",")
        # Get the id of the timeserie
        timeseriesId = values[0].strip() +\
            "_" + values[1].strip() + "_" + values[3].strip()
        # Get the time of the first value
        values[7] = values[7].replace("JAN", "01")
        values[7] = values[7].replace("FEB", "02")
        values[7] = values[7].replace("MRT", "03")
        values[7] = values[7].replace("APR", "04")
        values[7] = values[7].replace("MEI", "05")
        values[7] = values[7].replace("JUN", "06")
        values[7] = values[7].replace("JUL", "07")
        values[7] = values[7].replace("AUG", "08")
        values[7] = values[7].replace("SEP", "09")
        values[7] = values[7].replace("OKT", "10")
        values[7] = values[7].replace("NOV", "11")
        values[7] = values[7].replace("DEC", "12")
        values[7] = values[7].replace("MET", "")
        values[7] = values[7].strip()
        timeFirstValue = datetime.strptime(values[7], "%d-%m-%y %H:%M") -\
            timedelta(0, 0, 0, 0, 60)
        # Get all the measurements
        measurements = data[i].split(",")
        quality = data_quality[i].split(",")

        if len(measurements) != 7:
            raise Exception("Invalid number of measurements for timeserie.")

        if len(quality) != 7:
            raise Exception("Invalid number of quality flags for timeserie.")

        counter = 0
        for j in range(6):
            value = measurements[j].strip()
            value_flag = quality[j].strip()
            if value != "f" and value != "n":
                TimeForValue = timeFirstValue +\
                    timedelta(0, 0, 0, 0, interval * j)
                val_series.append(float(value))
                timestamp_series.append(TimeForValue)
                remoteid_series.append(timeseriesId)
                counter += 1
            if value_flag in [10, 30, 50, 70]:
                quality_series.append('0')
            elif value_flag in [2, 22, 24, 28, 42, 44, 48, 62, 68]:
                quality_series.append('3')
            else:
                quality_series.append('6')

    tsobj = DataFrame([timestamp_series, remoteid_series,
                       val_series, quality_series])
    tsobj = tsobj.transpose()
    tsobj.columns = ['tstamp', 'SensorID', 'value', 'flag']
    tsobj_indexed = tsobj.set_index(tsobj['tstamp'])
    return tsobj_indexed
