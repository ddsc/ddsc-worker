# (c) N&S. MIT licensed, see LICENSE.rst.
# Base Django settings, suitable for production.
# Imported (and partly overridden) by developmentsettings.py which also
# imports localsettings.py (which isn't stored in svn).  Buildout takes care
# of using the correct one.
# So: "DEBUG = TRUE" goes into developmentsettings.py and per-developer
# database ports go into localsettings.py.  May your hear turn purple if you
# ever put personal settings into this file or into developmentsettings.py!

import os

STATICFILES_FINDERS = (
    'staticfiles.finders.FileSystemFinder',
    'staticfiles.finders.AppDirectoriesFinder',
    # Enable 'old' /media directories in addition to /static.
    'staticfiles.finders.LegacyAppDirectoriesFinder',
    # Enable support for django-compressor.
    'compressor.finders.CompressorFinder',
)

# Set matplotlib defaults.
# Uncomment this when using lizard-map.
# import matplotlib
# # Force matplotlib to not use any Xwindows backend.
# matplotlib.use('Agg')
# import lizard_map.matplotlib_settings

# SETTINGS_DIR allows media paths and so to be relative to this settings file
# instead of hardcoded to c:\only\on\my\computer.
SETTINGS_DIR = os.path.dirname(os.path.realpath(__file__))

# BUILDOUT_DIR is for access to the "surrounding" buildout, for instance for
# BUILDOUT_DIR/var/static files to give django-staticfiles a proper place
# to place all collected static files.
BUILDOUT_DIR = os.path.abspath(os.path.join(SETTINGS_DIR, '..'))

# Production, so DEBUG is False. developmentsettings.py sets it to True.
DEBUG = False
# Show template debug information for faulty templates.  Only used when DEBUG
# is set to True.
TEMPLATE_DEBUG = True

# ADMINS get internal error mails, MANAGERS get 404 mails.
ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)
MANAGERS = ADMINS

# TODO: Switch this to the real production database.
# ^^^ 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
# In case of geodatabase, prepend with:
# django.contrib.gis.db.backends.(postgis)
DATABASES = {
    'default': {
        'NAME': '',      # via localproductionsettings!
        'ENGINE': '',    # via localproductionsettings!
        'USER': '',      # via localproductionsettings!
        'PASSWORD': '',  # via localproductionsettings!
        'HOST': '',      # via localproductionsettings!
        'PORT': '',      # via localproductionsettings!
        }
    }

# Almost always set to 1.  Django allows multiple sites in one database.
SITE_ID = 1

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name although not all
# choices may be available on all operating systems.  If running in a Windows
# environment this must be set to the same as your system time zone.
TIME_ZONE = 'UTC'
USE_TZ = True  # add back, otherwise ddsc_worker doesnt work for timestamp comparision
# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'nl-NL'
# For at-runtime language switching.  Note: they're shown in reverse order in
# the interface!
LANGUAGES = (
#    ('en', 'English'),
    ('nl', 'Nederlands'),
)
# If you set this to False, Django will make some optimizations so as not to
# load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds user-uploaded media.
MEDIA_ROOT = os.path.join(BUILDOUT_DIR, 'var', 'media')
# Absolute path to the directory where django-staticfiles'
# "bin/django build_static" places all collected static files from all
# applications' /media directory.
STATIC_ROOT = os.path.join(BUILDOUT_DIR, 'var', 'static')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
MEDIA_URL = '/media/'
# URL for the per-application /media static files collected by
# django-staticfiles.  Use it in templates like
# "{{ MEDIA_URL }}mypackage/my.css".
STATIC_URL = '/static_media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = ''  # via localproductionsettings!

ROOT_URLCONF = 'ddsc_worker.urls'

TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'

CACHES = {
    'default': {
        'KEY_PREFIX': BUILDOUT_DIR,
        'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
        'LOCATION': '127.0.0.1:11211',
    }
}

MIDDLEWARE_CLASSES = (
    # Gzip needs to be at the top.
    'django.middleware.gzip.GZipMiddleware',
    # Below is the default list, don't modify it.
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Lizard security.
    'tls.TLSRequestMiddleware',
    'lizard_security.middleware.SecurityMiddleware',
    )

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'lizard_security.backends.DDSCPermissionBackend',
)

INSTALLED_APPS = (
    # Default apps
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    #
    'django.contrib.admin',
    #
    'lizard_security',
#   'lizard_ui',  # after lizard-security
    'ddsc_worker',
    'ddsc_core',  # after lizard-security
    'south',
    'compressor',
    'staticfiles',
    'raven.contrib.django',
    'django_extensions',
    #
    'django.contrib.gis',
    'django.contrib.markup',
)

# TODO: Put your real url here to configure Sentry.
SENTRY_DSN = 'http://some:thing@sentry.lizardsystem.nl/1'

# TODO: add gauges ID here. Generate one separately for the staging, too.
UI_GAUGES_SITE_ID = ''  # Staging has a separate one.

try:
    # For local production overrides (DB passwords, for instance):
    from ddsc_worker.localproductionsettings import *  # NOQA
except ImportError:
    pass
