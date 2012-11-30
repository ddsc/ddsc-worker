from ddsc_worker.developmentsettings import *

DATABASES = {
    'default': {
        'NAME': 'ddsc',
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'USER': 'buildout',
        'PASSWORD': 'buildout',
        'HOST': '',  # empty string for localhost.
        'PORT': '',  # empty string for default.
    }
}

CASSANDRA = {
    'servers': [
        '10.11.12.14:9160',
    ],
    'keyspace': 'ddsc',
    'batch_size': 10000,
}
