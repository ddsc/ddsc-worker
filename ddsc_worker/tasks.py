from __future__ import absolute_import

import logging
import os
import string
import time
from datetime import datetime
import urllib2
import zipfile
import cStringIO

from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core import management

import pytz

from django.utils import timezone
from datetime import timedelta
from ddsc_core.models.models import Timeseries
from ddsc_core.models.models import StatusCache

from ddsc_logging.handlers import DDSCHandler

from ddsc_worker.celery import celery
from ddsc_worker.import_auth import get_usr_by_folder
from ddsc_worker.import_auth import get_usr_by_ip
from ddsc_worker.importer import data_move
from ddsc_worker.importer import file_ignored
from ddsc_worker.importer import import_csv
from ddsc_worker.importer import import_file
from ddsc_worker.importer import import_geotiff
from ddsc_worker.importer import import_pi_xml
from ddsc_worker.importer import import_lmw
#from ddsc_worker.importer import write2_cassandra

pd = getattr(settings, 'IMPORTER_PATH')
DestinationPath = pd['lmw']

lmw_url = getattr(settings, 'LMW_URL')
LmwUrl = lmw_url['url']

ams_tz = pytz.timezone('Europe/Amsterdam')


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
    src = pathDir + fileName
    fileDirName, fileExtension = os.path.splitext(src)
    fileExtension = string.lower(fileExtension)

    usr = get_usr_by_folder(pathDir)

    if usr == 0:
        data_move(src, (pd['storage_base_path'] + pd['rejected_file']))
        return
    else:
        logger.info('[x] start importing: %r' % src)
        logger.info('By User: %r' % usr.username)

    if fileExtension == ".filepart":
        fileName = fileName.replace(".filepart", "")
        src = pathDir + fileName
        fileDirName, fileExtension = os.path.splitext(src)
    if fileExtension == ".csv":
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
        dst = pd['storage_base_path'] + pd['pdf']
        import_file(pathDir, fileName, dst, usr.id)
    elif (fileExtension == ".zip"):
        dst = pd['storage_base_path'] + pd['geotiff']
        import_geotiff(pathDir, fileName, dst, usr.id)
    elif (fileExtension == ".xml"):
        import_pi_xml(src, usr.id)
    else:
        file_ignored(src, fileExtension)


@celery.task
def new_socket_detected(pathDir, fileName):
    src = pathDir + fileName
    usr = get_usr_by_ip(fileName)

    if usr is False:
        data_move(src, (pd['storage_base_path'] + pd['rejected_file']))
        raise Exception("[x] %r _FAILED to be imported" % src)
        return

    logger.info('[x] start importing: %r' % src)
    logger.info('By User: %r' % usr.username)
    import_csv(src, usr.id)


@celery.task
def download_lmw():

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

        # one day we should add the KWA data as well!
    except:
        logger.error("Error opening or extracting LMW zip file.")
        raise Exception("Error opening or extracting LMW zip file")

    celery.send_task("ddsc_worker.tasks.new_lmw_downloaded",
        kwargs={
            'pathDir': DestinationPath,
            'admFilename': admFileName,
            'datFilename': datFileName,
            'kwaFilename': kwaFileName,
        }
    )

    logger.info("LMW data successfully downloaded and send for processing ("
        + admFileName + " + " + datFileName + " + " + kwaFileName + ")")


@celery.task
def new_lmw_downloaded(pathDir, admFilename, datFilename, kwaFilename):
    import_lmw(pathDir, admFilename, datFilename, kwaFilename)


@celery.task
def calculate_status():
    days_cache = 30
    for ts in Timeseries.objects.all():
        now = timezone.now() - timedelta(1)
        first = now - timedelta(days_cache)
        ts_latest = ts.latest_value_timestamp
        if ts_latest != None:
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
                                    timeseries=ts, status_date=date_cursor)
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
