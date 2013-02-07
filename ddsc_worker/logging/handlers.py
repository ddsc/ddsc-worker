from __future__ import absolute_import

import logging

from ddsc_worker.logging.tasks import log


class DDSCHandler(logging.Handler):

    def emit(self, record):
        log.delay(self.format(record))
