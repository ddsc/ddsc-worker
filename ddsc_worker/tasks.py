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
    elif (fileExtension == ".png") or\
    (fileExtension == ".jpg") or\
    fileExtension == ".jpeg":
        dst = pd['storage_base_path'] + pd['image']
        import_file(pathDir, fileName, dst, usr.id)
    elif fileExtension == ".avi" or \
    fileExtension == ".wmv":
        dst = pd['storage_base_path'] + pd['video']
        import_file(pathDir, fileName, dst, usr.id)
    elif fileExtension == ".pdf":
        dst = pd['storage_base_path'] + pd['pdf']
        import_file(pathDir, fileName, dst, usr.id)
    elif (fileExtension == ".tif" or
    fileExtension == ".tiff"):
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
def DownloadLMW():

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

        with zipped.open("update.kwa", 'r') as datData:
            with open(datFileName, 'w') as kwaFile:
                kwaFile.write(datData.read())

        # one day we should add the KWA data as well!
    except:
        logger.error("Error opening or extracting LMW zip file.")
        raise Exception("Error opening or extracting LMW zip file")

    new_lmw_downloaded.delay(DestinationPath, admFileName,
                             datFileName, kwaFileName)
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
