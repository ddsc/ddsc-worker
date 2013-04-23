# (c) Fugro GeoServices. MIT licensed, see LICENSE.rst.
from ddsc_worker.settings import *

DATABASES = {
    # Changed server from production to staging
    'default': {
        'NAME': '',      # via localstagingsettings!
        'ENGINE': '',    # via localstagingsettings!
        'USER': '',      # via localstagingsettings!
        'PASSWORD': '',  # via localstagingsettings!
        'HOST': '',      # via localstagingsettings!
        'PORT': '',      # via localstagingsettings!
        },
    }

# TODO: add staging gauges ID here.
UI_GAUGES_SITE_ID = ''  # Staging has a separate one.

try:
    from ddsc_worker.localstagingsettings import *
    # For local staging overrides (DB passwords, for instance)
except ImportError:
    pass
