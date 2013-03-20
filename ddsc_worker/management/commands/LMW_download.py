# This script should be run by crontab and executed every 10 minutes
# The script downloads the latest LMW data from the server
# Then, a task is started to parse this data
import urllib2
import zipfile
import cStringIO
from datetime import datetime
from django.conf import settings
import logging
# still not sure which celery should be called
from ddsc_worker.celery import celery

logger = logging.getLogger(__name__)

# SETTINGS
path_setting = getattr(settings, 'IMPORTER_PATH')
DestinationPath = path_setting['lmw']

lmw_url = getattr(settings, 'LMW_URL')
LmwUrl = lmw_url['url']


def DownloadLMW():

    baseFileName = "lmw_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    admFileName = DestinationPath + "/" + baseFileName + ".adm"
    datFileName = DestinationPath + "/" + baseFileName + ".dat"

    try:
        # Connect to the file location at the RWS server
        connection = urllib2.urlopen(LmwUrl)

        # Read the file and put the data into a StringIO
        stream = cStringIO.StringIO(connection.read())
    except:
        # TODO: HERE SHOULD BE LOGGING
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

        # one day we should add the KWA data as well!
    except:
        # TODO: HERE SHOULD BE LOGGING
        logger.error("Error opening or extracting LMW zip file.")
        raise Exception("Error opening or extracting LMW zip file")

    celery.send_task("ddsc_worker.tasks.new_lmw_downloaded",
        kwargs={
            'pathDir': DestinationPath,
            'admFileName': admFileName,
            'datFileName': datFileName
        }
    )

    logger.info("LMW data successfully downloaded and send for processing ("
        + admFileName + " + " + datFileName + ")")

    return

DownloadLMW()
