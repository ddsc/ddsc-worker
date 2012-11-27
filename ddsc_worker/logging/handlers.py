from __future__ import absolute_import
from ddsc_worker.logging.tasks import log
import logging


class DDSCHandler(logging.Handler):

    def emit(self, record):
        log.delay(self.format(record))
