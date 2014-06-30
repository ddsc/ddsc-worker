# (c) Fugro GeoServices. MIT licensed, see LICENSE.rst.
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
from ddsc_worker.geoserver.catalog import Catalog
logger = logging.getLogger(__name__)

pd = getattr(settings, 'IMPORTER_PATH')
ERROR_file = pd['storage_base_path'] + pd['rejected_file']
OK_file = pd['storage_base_path'] + pd['accepted_file']
gs_setting = getattr(settings, 'IMPORTER_GEOSERVER')


def data_convert(src):
    try:
        logger.debug("[x] converting %r to pandas object", src)
        # Half the time read_csv fails with: "IndexError: list index out of
        # range". Quite bizar, but asking for some file metadata first,
        # e.g. the file size, seems to solve the problem. Performing
        # a sleep does not solve the problem. Something fishy about
        # accessing a file on a mounted Windows share from Ubuntu?
        logger.debug("%s is %d bytes", src, os.path.getsize(src))
        tsOBJ = read_csv(
            src, index_col=0,
            parse_dates=True,
            names=['SensorID', 'value'],
        )
    except Exception, e:
        logger.error(e, exc_info=True)
        logger.error("[x] %r _FAILED to be converted", src)
        data_move(src, ERROR_file)
        raise e
    tsOBJ = tsOBJ.sort()
    logger.debug("[x] %r _converted & sorted", src)
    return tsOBJ


def write2_cassandra(tsOBJ_yes, ts, src):
    try:
        tsOBJ_yes = tsOBJ_yes.tz_localize('UTC')
        del tsOBJ_yes['SensorID']
        ts.set_events(tsOBJ_yes)
        ts.save()
        logger.debug("[x] %r _written" % (src))
    except Exception, e:
        logger.error(
            "[x] %r _FAILED to be written to cassandra",
            src, exc_info=True
        )
        data_move(src, ERROR_file)
        raise e


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
    auth_tag = 0
    for name, tsobj_grouped in tsgrouped:
        remoteid = tsobj_grouped['SensorID'][0]
        ts = get_auth(usr, remoteid)
        if ts is False:
            logger.debug(
                '[x] Timeseries %r has been rejected because of authorization',
                remoteid
            )
            auth_tag += 1
        else:
            write2_cassandra(tsobj_grouped, ts, src)
    if str(auth_tag) == nr:
        logger.error('[x] File:--%r-- has been fully rejected' % src)
        data_move(src, ERROR_file)
        raise Exception("[x] In : %r  All of the timeseries failed"
            " to be imported because of auth" % (src))
    elif auth_tag == 0:
        data_move(src, OK_file)
        logger.info('[x] File:--%r-- has been successfully imported' % src)
    else:
        data_move(src, ERROR_file)
        logger.warning('[x] File:--%r-- has been only partly imported' % src)
        raise Exception("[x] In : %r  Some of the timeseries failed"
            " to be imported because of auth" % (src))


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

    uuid = get_remoteid_by_filename(filename)
    filename_without_ext =  filename.replace('.geotiff', '')
    ts = get_auth(usr, uuid)

    logger.debug("[x] publishing %r into GeoServer..." % src)

    if ts is not False:
        str_year = str(timestamp.year[0])
        str_month = str(timestamp.month[0])
        str_day = str(timestamp.day[0])
        store_dst = dst + uuid + '/' + \
        str_year + '-' + str_month + '-' + str_day + '/'

        cat = Catalog(gs_setting['geoserver_url'] + '/rest', 
            gs_setting['geoserver_username'], 
            gs_setting['geoserver_password'], True)
            
        workspace = cat.get_workspace(gs_setting['geoserver_workspace'])
        if workspace == None:
            cat.create_workspace(gs_setting['geoserver_workspace'], gs_setting['geoserver_workspace'])

        try:
            data_move(src, store_dst)
            #store = cat.create_coveragestore(filename_without_ext, store_dst + filename, workspace, True)
            create_geotiff(cat, gs_setting['geoserver_workspace'], filename_without_ext, store_dst + filename)
        except Exception, e:
            data_move(src, ERROR_file)
            logger.error('[x] File:--%r-- has been filed to publish' % src)
            raise Exception("[x] %r _FAILED to be imported" % src + ' reason--%r' % e )

        values = {"value": store_dst + filename}

        logger.debug("[x] Published %r to GeoServer " % src)
        ts.set_event(timestamp[0], values)
        ts.save()
        logger.info("[x] File:--%r-- has been successfully imported" % src)
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
                del df['flag']
                ts.set_events(df)
                ts.save()
                logger.debug("[x] %r _written" % (src))
            except:
                logger.error(
                    "[x] %r _FAILED to be written to cassandra" % (src))
                data_move(src, ERROR_file)
                raise Exception('piXML file: %r ERROR to convert!' % src)

    data_move(src, OK_file)
    logger.info('[x] File:--%r-- has been successfully imported' % src)


def file_ignored(src, fileExtension):
    logger.error('[x]--Warning-- * %r' % fileExtension +
        ' FILE: %r is not acceptable' % src)
    dst = pd['storage_base_path'] + pd['rejected_file']
    data_move(src, dst)
    raise Exception("[x] %r _FAILED to be imported" % src)


def import_lmw(DestinationPath, admFileName, datFileName, kwaFileName):
    ## get the user which in this case should be LMW I guess
    usr = User.objects.get(username='lmw_ddsc')
    adm_src = admFileName
    dat_src = datFileName
    kwa_src = kwaFileName

    tsobj_indx = read_lmw(adm_src, dat_src, kwa_src)
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

    data_move(adm_src, OK_file)
    data_move(dat_src, OK_file)
    data_move(kwa_src, OK_file)
    logger.info('[x] File:--%r-- has been successfully imported' % adm_src)


def read_lmw(admFile, datFile, kwaFile):
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
    zom_win = []

    for i in range(len(administration)):
        values = administration[i].split(",")
        # Get the id of the timeserie
        timeseriesId = values[0].strip() +\
            "_" + values[1].strip() + "_" + values[3].strip()
        # Get the time of the first value
        if values[7].find('MET') == -1:
            zom_win = 'summer'

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
        values[7] = values[7].replace("Z03", "")
        values[7] = values[7].replace("MET", "")
        values[7] = values[7].strip()

        if zom_win == 'summer':
            timeFirstValue = datetime.strptime(values[7], "%d-%m-%y %H:%M") -\
                timedelta(0, 0, 0, 0, 120)
        else:
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
            value_flag = int(quality[j])
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

    tsobj = DataFrame([remoteid_series,
                       val_series, quality_series])
    tsobj = tsobj.transpose()
    tsobj.columns = ['SensorID', 'value', 'flag']
    tstamp = DataFrame(timestamp_series, columns=['ts'])
    tsobj_indexed = tsobj.set_index(tstamp['ts'])
    return tsobj_indexed


def make_file_name_unique(file_name):
    pos = file_name.find('.')
    time_string = datetime.utcnow().strftime("%Y%m%d%H%M%S_%f")
    new_name = file_name[0:pos] + '_' + time_string + file_name[pos:]
    return new_name


def create_geotiff(catelog, work_space, store_name, file_path):
     headers = {
            "Content-type": "application/xml",
            "Accept": "application/xml"
     }
     message = "url:file:" + file_path

     url = catelog.service_url + "/workspaces/" +\
         work_space + "/coveragestores/" +\
         store_name + "/external.geotiff"
     response, content = catelog.http.request(url, "PUT", message, headers)

     if (response.status != 201):
         raise Exception(content)
