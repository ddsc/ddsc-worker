from ddsc_worker.settings import *

DATABASES = {
    # Changed server from production to staging
    'default': {
        'NAME': 'ddsc_worker',
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'USER': 'ddsc_worker',
        'PASSWORD': '3sqsk3+!z0',
        'HOST': 's-web-db-00-d03.external-nens.local',
        'PORT': '5432',
        },
    }

# TODO: add staging gauges ID here.
UI_GAUGES_SITE_ID = ''  # Staging has a separate one.
