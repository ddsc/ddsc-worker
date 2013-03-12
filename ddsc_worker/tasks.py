from __future__ import absolute_import

import logging
import os
import string
import time

from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core import management
#from pandas.io.parsers import read_csv

from ddsc_core.models import Timeseries
from ddsc_logging.handlers import DDSCHandler
from tslib.readers import PiXmlReader

from ddsc_worker.celery import celery
from ddsc_worker.import_auth import get_usr_by_folder
from ddsc_worker.import_auth import get_usr_by_ip
#from ddsc_worker.importer import data_delete
from ddsc_worker.importer import data_move
from ddsc_worker.importer import file_ignored
from ddsc_worker.importer import import_csv
from ddsc_worker.importer import import_file
from ddsc_worker.importer import import_geotiff
#from ddsc_worker.importer import write2_cassandra

pd = getattr(settings, 'IMPORTER_PATH')


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
    time.sleep(9)
    return x + y


@celery.task(ignore_result=True)
def import_pi_xml(src):
    logger.info("Importing %r" % src)
    reader = PiXmlReader(src)
    for md, df in reader.get_series():
        code = md['header']['parameterId']
        ts, _ = Timeseries.objects.get_or_create(code=code)
        ts.set_events(df)
        ts.save()
    return src


@celery.task(ignore_result=True)
def export_pi_xml(src, dst):
    logger.info("Exporting %r" % src)
    management.call_command("export_pi_xml", src, file=dst)


# The lmw importing task implementation yet to be decided
#@celery.task
#def import_lmw(src, fileName):
#    date_spec = {"timedate": [0, 1]}
#    tsOBJ = read_csv(src,
#                     skiprows=6, parse_dates=date_spec,
#                     sep=";", index_col=0, header=None)
#
#    tsOBJ = tsOBJ.rename(
#        columns={2: 'location', 3: 'met', 4: 'q_flag', 5: 'value'}
#    )
#
#    f = open(src, 'r')
#
#    str = f.readline()
#    strlist = str.split('=')
#    data_bank = strlist[1].replace("\r\n", '')
#
#    str = f.readline()
#    strlist = str.split('=')
#    location = strlist[1].replace("\r\n", '')
#
#    str = f.readline()
#    strlist = str.split('=')
#    waarnemingsgroepcode = strlist[1].replace("\r\n", '')
#
#    str = f.readline()
#    strlist = str.split('=')
#    x_cord = strlist[1].replace("\r\n", '')
#
#    str = f.readline()
#    strlist = str.split('=')
#    y_cord = strlist[1].replace("\r\n", '')
#
#    i = 0
#    tsOBJ['flag'] = 'None'
#    for row in tsOBJ.iterrows():
#            if tsOBJ.q_flag[i] in [10, 30, 50, 70]:
#                tsOBJ['flag'][i] = '0'
#                i += 1
#            elif tsOBJ.q_flag[i] in [2, 22, 24, 28, 42, 44, 48, 62, 68]:
#                tsOBJ['flag'][i] = '3'
#                i += 1
#            else:
#                tsOBJ['flag'][i] = '6'
#                i += 1
#    logger.info("[x] %r _validated" % (src))
#    tsOBJ = tsOBJ.tz_localize('UTC')
#    del tsOBJ['location']
#    del tsOBJ['met']
#    del tsOBJ['q_flag']
#
#    st = write2_cassandra(tsOBJ, src)
#
#    data_delete(st, src)


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
    else:
        file_ignored.delay(src, fileExtension)


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
