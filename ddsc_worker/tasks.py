# (c) Fugro GeoServices. MIT licensed, see LICENSE.rst.
from __future__ import absolute_import

from datetime import datetime, timedelta
import cStringIO
import csv
import logging
import os
import smtplib
import string
import time
import urllib2
import zipfile

from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core import management
from django.template import Context
from django.template import Template
from django.utils import timezone
import pytz

from ddsc_core.models.alarms import Alarm
from ddsc_core.models.alarms import Alarm_Active
from ddsc_core.models.alarms import Alarm_Item
from ddsc_core.models.aquo import Unit
from ddsc_core.models.models import StatusCache
from ddsc_core.models.models import Timeseries

from ddsc_logging.handlers import DDSCHandler

from ddsc_worker.celery import celery
from ddsc_worker.import_auth import get_usr_by_folder
from ddsc_worker.import_auth import get_usr_by_ip
from ddsc_worker.importer import data_move
from ddsc_worker.importer import file_ignored
from ddsc_worker.importer import import_csv
from ddsc_worker.importer import import_file
from ddsc_worker.importer import import_geotiff
from ddsc_worker.importer import import_lmw
from ddsc_worker.importer import import_pi_xml
from ddsc_worker.importer import make_file_name_unique


SMTP_SETTINGS = getattr(settings, 'SMTP')
SMTP_HOST = SMTP_SETTINGS['host']
SMTP_PORT = SMTP_SETTINGS['port']
FROM_ADDRESS = SMTP_SETTINGS['sender']

compensation = getattr(settings, 'COMPENSATION')
COMPENSATION_CSV_PATH = compensation['csv_path']

pd = getattr(settings, 'IMPORTER_PATH')
DestinationPath = pd['lmw']

lmw_url = getattr(settings, 'LMW_URL')
LmwUrl = lmw_url['url']

ams_tz = pytz.timezone('Europe/Amsterdam')

ALARM = getattr(settings, 'ALARM')
timelag_min = ALARM['timelag']
timelag = timedelta(0, 0, 0, 0, int(timelag_min), 0, 0)


@after_setup_task_logger.connect
def setup_ddsc_task_logger(**kwargs):
    """Log records in the ddsc_worker package to RabbitMQ.

    The logging level is inherited from the root logger (and will be `WARNING`
    if you accept the default when running Celery as a daemon).

    Records will be logged to a topic exchange (ddsc.log).

    """
    handler = DDSCHandler(celery.conf['BROKER_URL'])
    logger = logging.getLogger('ddsc_worker')
    logger.addHandler(handler)


logger = get_task_logger(__name__)


@celery.task
def add(x, y):
    logger.debug("Adding %r + %r" % (x, y))
    time.sleep(20)
    return x + y


@celery.task(acks_late=True, ignore_result=True)
def export_pi_xml(src, dst, **options):
    """Create a pi.xml file based on a template.

    A template is a valid pi.xml file in which each series has a `comment`
    element containing a uuid of a ddsc timeseries. A template generally
    contains no events (they will be ignored anyway).

    Keyword arguments:
    src -- path to a template pi.xml
    dst -- path to the resulting pi.xml

    """
    logger.info("Exporting %r to %r" % (src, dst))
    management.call_command("export_pi_xml", src, file=dst, **options)


@celery.task
def new_file_detected(pathDir, fileName):
    try:
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
        fileExtension = string.lower(fileExtension)

        usr = get_usr_by_folder(pathDir)

        if usr == 0:
            data_move(src, (pd['storage_base_path'] + pd['rejected_file']))
            logger.error('[x] User-folder does not exist')
            raise Exception('Failed to be authorized!')
        else:
            logger.info('[x] start importing: %r' % src)
            logger.info('By User: %r' % usr.username)

        if fileExtension == ".filepart":
            fileName = fileName.replace(".filepart", "")
            src = pathDir + fileName
            fileDirName, fileExtension = os.path.splitext(src)
        if fileExtension == ".csv":
            new_fileName = make_file_name_unique(fileName)
            os.rename(pathDir + fileName, pathDir + new_fileName)
            fileName = new_fileName
            src = pathDir + new_fileName
            import_csv(src, usr.id)
        elif (fileExtension == ".png") or \
        (fileExtension == ".jpg") or \
        fileExtension == ".jpeg" or fileExtension == ".tif" or \
        fileExtension == ".tiff":
            dst = pd['storage_base_path'] + pd['image']
            import_file(pathDir, fileName, dst, usr.id)
        elif fileExtension == ".avi" or \
        fileExtension == ".wmv":
            dst = pd['storage_base_path'] + pd['video']
            import_file(pathDir, fileName, dst, usr.id)
        elif fileExtension == ".pdf":
            dst = pd['storage_base_path'] + pd['file']
            import_file(pathDir, fileName, dst, usr.id)
        elif (fileExtension == ".zip"):
            dst = pd['storage_base_path'] + pd['geotiff']
            import_geotiff(pathDir, fileName, dst, usr.id)
        elif (fileExtension == ".xml"):
            new_fileName = make_file_name_unique(fileName)
            os.rename(pathDir + fileName, pathDir + new_fileName)
            src = pathDir + new_fileName
            fileName = new_fileName
            import_pi_xml(src, usr.id)
        else:
            file_ignored(src, fileExtension)
    except:
        x = pd['storage_base_path'] + pd['rejected_file']
        y = fileName
        new_file_detected.apply_async((x, y), queue='ddsc.failures')
        raise Exception('task has been requeued to ddsc-failure')


@celery.task
def new_socket_detected(pathDir, fileName):
    try:
        src = os.path.join(pathDir, fileName)
        str_path = pd['storage_base_path'] + pd['rejected_file']
        usr = get_usr_by_ip(fileName)

        if usr is False:
            data_move(src, str_path)
            raise Exception("[x] %r _FAILED to be authorized" % src)

        logger.info('[x] start importing: %r', src)
        logger.info('By User: %r', usr.username)
        #time.sleep(5)  # why sleep?
        import_csv(src, usr.id)
    except Exception, e:
        logger.error("Unable to process socket data %s", src, exc_info=True)
        new_socket_detected.apply_async((str_path, fileName),
                queue='ddsc.failures')
        raise e


@celery.task
def download_lmw():
    logger.info('[x] Starting LMW downloading...')
    baseFileName = "lmw_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    admFileName = DestinationPath + baseFileName + ".adm"
    datFileName = DestinationPath + baseFileName + ".dat"
    kwaFileName = DestinationPath + baseFileName + ".kwa"

    try:
        # Connect to the file location at the RWS server
        connection = urllib2.urlopen(LmwUrl)

        # Read the file and put the data into a StringIO
        stream = cStringIO.StringIO(connection.read())
    except:
        logger.error("Could not download LMW data from RWS-LMW server.")
        raise Exception("Could not download LMW data from RWS-LMW server.")

    try:
        # Open the zip file
        zipped = zipfile.ZipFile(stream, 'r')
        # Extract the two files of interest
        # and write the data to the file system
        with zipped.open("update.adm", 'r') as admData:
            with open(admFileName, 'w') as admFile:
                admFile.write(admData.read())

        with zipped.open("update.dat", 'r') as datData:
            with open(datFileName, 'w') as datFile:
                datFile.write(datData.read())

        with zipped.open("update.kwa", 'r') as kwaData:
            with open(kwaFileName, 'w') as kwaFile:
                kwaFile.write(kwaData.read())

    except:
        logger.error("Error opening or extracting LMW zip file.")
        raise Exception("Error opening or extracting LMW zip file")

    new_lmw_downloaded(
        pathDir=DestinationPath,
        admFilename=admFileName,
        datFilename=datFileName,
        kwaFilename=kwaFileName
    )

    logger.info("LMW data successfully downloaded and send for processing ("
        + admFileName + " + " + datFileName + " + " + kwaFileName + ")")


@celery.task
def new_lmw_downloaded(pathDir, admFilename, datFilename, kwaFilename):
    try:
        import_lmw(pathDir, admFilename, datFilename, kwaFilename)
    except Exception, e:
        logger.error("LMW download failed", exc_info=True)
        new_lmw_downloaded.apply_async((pathDir, admFilename,
                datFilename, kwaFilename),
                queue='ddsc.failures')
        raise e


@celery.task
def calculate_status():
    logger.info('[x] Starting Status Calculating...')
    days_cache = 30
    for ts in Timeseries.objects.all():
        now = timezone.now() - timedelta(1)
        first = now - timedelta(days_cache)
        ts_latest = ts.latest_value_timestamp
        if ts_latest is not None:
            if (now - ts_latest).days < days_cache:
                for i in range(1, days_cache + 1):
                    start = datetime(first.year,
                                     first.month, first.day, 0, 0, 0) +\
                                     timedelta(i)
                    end = datetime(first.year,
                                   first.month, first.day, 0, 0, 0) +\
                                   timedelta(i + 1)
                    ts_latest = ts_latest.replace(tzinfo=None)
                    if ts_latest > start:
                        date_cursor = start.strftime('%Y-%m-%d')
                        try:
                            start = ams_tz.localize(start)
                            end = ams_tz.localize(end)
                            tsobj = ts.get_events(start, end)
                            if tsobj.empty is False:
                                st, cr = StatusCache.objects.get_or_create(
                                    timeseries=ts, date=date_cursor)
                                st.set_ts_status(tsobj)
                                st.save()
                                logger.debug(
                                    'current timeseries: %r has' % ts.uuid +
                                    'value for Date: %r' % date_cursor)
                        except:
                            logger.debug(
                                'current timeseries: %r has' % ts.uuid +
                                ' no value for Date: %r' % date_cursor)
    logger.info('[x] Complete updating status for %r' % now.strftime(
                                                            '%Y-%m-%d'))


@celery.task
def alarm_trigger():
    logger.info('[x] Starting Alarm Checking...')
    for alm in Alarm.objects.filter(active_status=True):
        try:
            alm_act_list = Alarm_Active.objects.\
                filter(alarm_id=alm.id).order_by('pk')
            ll = len(alm_act_list)
            alm_act = alm_act_list[ll - 1]
        except:
            logger.debug('no alarm active objects in current alarm')
            Alarm_Active.objects.create(
                alarm_id=alm.id,
                first_triggered_on=timezone.now(),
                message='none',
                active=False,
            )
            alm_act = Alarm_Active.objects.latest('pk')
        final_decision = False
        list_ts_info = ''
        current_time = timezone.now()
        if alm.last_checked is not None:
            time_diff = current_time - alm.last_checked
            time_diff_sec = abs(time_diff.total_seconds())
        else:
            time_diff_sec = 9999999
            alm.last_checked = timezone.now() - timedelta(9999)
            alm.save()
        alarm_or_not = []
        if (alm != [] and time_diff_sec > alm.frequency * 60):
            logger.info('executing, alarm name: %r' % alm.name)
            logical_check = alm.logical_check
            logger.debug('logical_check: %r' % logical_check)
            for alm_itm in Alarm_Item.objects.filter(alarm_id=alm.id):
                if alm_itm != []:
                    try:
                        ts_series = alm_itm.content_object.timeseries.all()
                    except:
                        if alm_itm.content_object is None:
                            continue
                        ts_series = [alm_itm.content_object]
                    logical_check_item = alm_itm.logical_check
                    alarm_or_not_item = []
                    for ts in ts_series:
                        logger.debug('ts_lastest timestamp: %r'
                            % ts.latest_value_timestamp)
                        logger.debug('alarm_last_checked timestamp: %r'
                            % alm.last_checked)
                        if ts.latest_value_timestamp is not None:
                            if ts.latest_value_timestamp + timelag > \
                                (alm.last_checked):
                                logger.debug('checking timeseries: %r' %
                                    ts.uuid + 'within alarm item id %r' %
                                    alm_itm.id)
                                logger.debug('comparison type is:'
                                    + '%r' % alm_itm.comparision)
                                logger.debug('comparison value type is: '
                                    + '%r' % alm_itm.value_type)
                                if alm_itm.value_type == Alarm_Item.ValueType\
                                                             .LATEST_VALUE:
                                    temp_deci = compare(alm_itm.comparision,
                                        alm_itm.value_double,
                                        ts.latest_value_number)
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries: ' + \
                                            ts.name + ' \t' + \
                                            'at ' + ts.latest_value_timestamp.\
                                            strftime(
                                                '%Y-%m-%d %H:%M:%S (UTC)') + \
                                            ' \t' + 'value:' +\
                                            str(ts.latest_value_number) +\
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.NR_MEASUR:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        nr_measur = st_cache\
                                            .nr_of_measurements_total
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_int, nr_measur)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Number of measurements:' + \
                                            str(nr_measur) + \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.PR_RELIABLE:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        nr_reliable = st_cache\
                                            .nr_of_measurements_reliable
                                        nr_measur = st_cache\
                                            .nr_of_measurements_total
                                        pr_reliable = nr_reliable / nr_measur * 100
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, pr_reliable)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Reliable measurements:' + \
                                            str(pr_reliable) + '%' \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '% \n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.PR_DOUBTFUL:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        nr_doubtful = st_cache\
                                                      .nr_of_measurements_doubtful
                                        nr_measur = st_cache\
                                            .nr_of_measurements_total
                                        pr_doubtful = nr_doubtful / nr_measur * 100
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, pr_doubtful)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Doubtful measurements:' + \
                                            str(pr_doubtful) + '%' \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '% \n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.PR_UNRELIABLE:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        nr_unreliable = st_cache\
                                            .nr_of_measurements_unreliable
                                        nr_measur = st_cache\
                                            .nr_of_measurements_total
                                        pr_unreliable = nr_unreliable / nr_measur\
                                            * 100
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, pr_unreliable)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Unreliable measurements:' + \
                                            str(pr_unreliable) + '%' \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '% \n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.MIN_MEASUR:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        min_measur = st_cache.min_val
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, min_measur)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Minimal measurements:' + \
                                            str(min_measur) + \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.MAX_MEASUR:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        max_measur = st_cache.max_val
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, max_measur)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Maximal measurements:' + \
                                            str(max_measur) + \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.AVG_MEASUR:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        avg_measur = st_cache.mean_val
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, avg_measur)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Mean measurements:' + \
                                            str(avg_measur) + \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.STD_MEASUR:
                                    try:
                                        st_cache = ts.statuscache_set.latest('pk')
                                        std_measur = st_cache.std_val
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_double, std_measur)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'STD of measurements:' + \
                                            str(std_measur) + \
                                            ' \t' + 'threshold:' + \
                                            str(alm_itm.value_double) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.TIME_SINCE_LAST_MEASUR:
                                    try:
                                        time_diff = timezone.now() -\
                                                        ts.latest_value_timestamp
                                        time_diff_sec = int(
                                            time_diff.total_seconds())
                                        temp_deci = compare(alm_itm.comparision,
                                            alm_itm.value_int, time_diff_sec)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Time elapse since last ' + \
                                            'measurement:' + \
                                            str(time_diff_sec) + ' Sec' + \
                                            ' \t' + 'threshold:' + 'Sec' + \
                                            str(alm_itm.value_int) + '\n'
                                elif alm_itm.value_type == Alarm_Item\
                                    .ValueType.PR_DEV_EXPECTED_NR_MEASUR:
                                    try:
                                        nr_measur = st_cache\
                                            .nr_of_measurements_total
                                        nr_expected_measur = alm_itm.value_int
                                        pr_expected_deviation = alm_itm\
                                            .value_double
                                        deviation = abs(
                                            nr_measur - nr_expected_measur)
                                        pr_deviation = float(deviation) /\
                                                           nr_expected_measur
                                        temp_deci = compare(alm_itm.comparision,
                                            pr_expected_deviation, pr_deviation)
                                    except:
                                        temp_deci = False
                                    alarm_or_not_item.append(temp_deci)
                                    if temp_deci is True:
                                        list_ts_info += 'Timeseries_status: ' \
                                            + ts.name + ' \t' + \
                                            'on ' + st_cache.date.\
                                            strftime(
                                                '%Y-%m-%d (UTC)') + ' \t' + \
                                            'Deviation from the expected ' + \
                                            'nr of measurements:' + \
                                            str(pr_deviation) + '%' + \
                                            ' \t' + 'threshold:' + '%' + \
                                            str(pr_expected_deviation) + '\n'
                            else:
                                logger.debug('current alarm item has'
                                    ' already been checked')
                if alarm_or_not_item != []:
                    ds = decision(alarm_or_not_item,
                        logical_check_item)
                    logger.debug('alarm or not (item level)? ...: %r'
                        % alarm_or_not_item)
                    logger.debug('alarm or not (item level) decision: '
                        '%r' % ds)
                    alarm_or_not.append(ds)
            alm.last_checked = timezone.now()
            super(Alarm, alm).save()
        else:
            logger.debug('current alarm has been checked recently!')
        if alarm_or_not != []:
            logger.debug('alarm or not (alarm level)? ...: %r' % alarm_or_not)
            final_decision = decision(alarm_or_not, logical_check)
            logger.debug('final alarm or not? '
                '%r' % final_decision)
            if final_decision is True:
                logger.info('[x] Alarm: ' + alm.name + ' with ID: ' +
                               str(alm.pk) + ' is being triggered')
                smtp = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
                from_addr = FROM_ADDRESS
                t = Template(alm.template)
                CONTEXT_DICT = {
                    'alarm_name': alm.name,
                    'urgency': alm.urgency,
                    'alarming_time': timezone.now(),  # utc time
                    'list_timeseries': list_ts_info,
                    'sender_name': alm.content_object,
                }
                c = Context(CONTEXT_DICT)
                msg = t.render(c)
                ### maintaining the alarm_active table ###
                if not alm_act.active:
                    Alarm_Active.objects.create(
                        alarm_id=alm.id,
                        first_triggered_on=timezone.now(),
                        message=msg,
                    )
                    if alm.message_type == Alarm.MessageType.EMAIL:
                        try:
                            user_list = alm.content_object.members.all()
                            for user in user_list:
                                to_addr = user.email
                                header = 'To:' + to_addr + '\n' + 'From: ' + \
                                    from_addr + '\n' + 'Subject: ALARM! \n'
                                msg = header + msg
                                smtp.sendmail(from_addr, to_addr, msg)
                        except:
                            user = alm.content_object
                            to_addr = user.email
                            header = 'To:' + to_addr + '\n' + 'From: ' + \
                                from_addr + '\n' + 'Subject: ALARM! \n'
                            msg = header + msg
                            smtp.sendmail(from_addr, to_addr, msg)
                else:
                    alm_act.message = msg
                    alm_act.save()
            else:
                if alm_act.active is True:
                        alm_act.active = False
                        alm_act.deactivated_on = timezone.now()
                        alm_act.save()
        logger.info('[x] Finish checking Alarm: %r ' % alm.name)


def compare(x, y, z):
    return {
        1: y == z,
        2: y != z,
        3: y < z,
        4: y > z,
    }.get(x, 'error')


def decision(x, y):
    return {
        0: sum(x) == len(x),
        1: sum(x) != 0,
    }.get(y, 'error')


@celery.task
def compensation_tool():
    f_csv = COMPENSATION_CSV_PATH
    logger.info('[x] Calculating compensation')
    with open(f_csv, 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            water_pr_id = row[0]
            air_pr_id = row[1]
            water_ht_id = row[2]
            ref_ht = row[3]
            cable_len = row[4]
            correction = row[5]
            max_timeDiff = row[6]
            max_timeDiff = timedelta(0, 0, 0, 0, int(max_timeDiff))
            water_dpt_id = row[7]

            try:
                ts_waterPr = Timeseries.objects.get(uuid=water_pr_id)
                ts_waterHt = Timeseries.objects.get(uuid=water_ht_id)
                ts_air_pr = Timeseries.objects.get(uuid=air_pr_id)
                ts_waterDpt = Timeseries.objects.get(uuid=water_dpt_id)
            except:
                logger.error('[x] current' +
                   ' compensation-related timeseries does not exsist!')
                raise Exception(
                            'current compensation-related' +
                            ' time series does not exsist')

            if ts_waterHt.first_value_timestamp is None:
                ts_waterHt.first_value_timestamp = timezone.now() - \
                    timedelta(9999)
                ts_waterHt.save()
            if ts_waterHt.latest_value_timestamp is None:
                ts_waterHt.latest_value_timestamp = \
                    ts_waterHt.first_value_timestamp + timedelta(1)
                ts_waterHt.latest_value_number = 0
                ts_waterHt.save()

            if ts_waterDpt.first_value_timestamp is None:
                ts_waterDpt.first_value_timestamp = timezone.now() - \
                    timedelta(9999)
                ts_waterDpt.save()
            if ts_waterDpt.latest_value_timestamp is None:
                ts_waterDpt.latest_value_timestamp = \
                    ts_waterHt.first_value_timestamp + timedelta(1)
                ts_waterDpt.latest_value_number = 0
                ts_waterDpt.save()

            if ts_waterHt.latest_value_timestamp <\
                ts_waterPr.latest_value_timestamp or\
                ts_waterDpt.latest_value_timestamp <\
                ts_waterPr.latest_value_timestamp:

                water_pr = ts_waterPr.latest_value_number
                start_air_timestamp = \
                    ts_waterPr.latest_value_timestamp - max_timeDiff
                end_air_timestamp = \
                    ts_waterPr.latest_value_timestamp + timedelta(0, 1)
                tsobj_airpr = ts_air_pr.get_events(start_air_timestamp,
                    end_air_timestamp)
                try:
                    air_pr = tsobj_airpr['value'][-1]
                except:
                    air_pr = None
                if air_pr is not None:
                    if ref_ht == '':
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        logger.error('reference hight unit' +
                            ' is not indicated')
                        raise Exception(
                            'reference hight unit is not indicated')
                    else:
                        ref_ht = float(ref_ht)
                    if cable_len == '':
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        logger.error('cable length' +
                            ' is not indicated')
                        raise Exception('cable length is not indicated')
                    else:
                        cable_len = float(cable_len)
                    if correction == '':
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        logger.error('correction value' +
                            ' is not indicated')
                        raise Exception(
                            'correction value is not indicated')
                    else:
                        correction = float(correction)
                    if ts_air_pr.unit != Unit.objects.get(code='mbar'):
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        raise Exception('air pressure unit is not mbar')
                    if ts_waterPr.unit != Unit.objects.get(code='mbar'):
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        raise Exception('water pressure unit is not mbar')
                    if ts_waterHt.unit != Unit.objects.get(code='m'):
                        compensation_tool.apply_async((),
                            queue='ddsc.failures')
                        raise Exception('water height unit is not meter')
                    A = ref_ht
                    B = cable_len
                    C = correction
                    signal_ba = water_pr
                    ba = air_pr

                    WS = (signal_ba - ba) / 98.06
                    row = {'value': WS}

                    ts_waterDpt.set_event(
                        ts_waterPr.latest_value_timestamp, row)
                    ts_waterDpt.save()

                    if C is None or A is None or B is None:
                        pass
                    else:
                        WS = (A - B) + (signal_ba - ba) / 98.06 - C
                        row = {'value': WS, 'flag': 'None'}
                        ts_waterHt.set_event(
                            ts_waterPr.latest_value_timestamp, row)
                        ts_waterHt.save()

    logger.info('[x] Finished calculating compensation')
