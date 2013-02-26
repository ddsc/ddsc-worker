from ddsc_worker.settings import *  # NOQA

DEBUG = True

# ENGINE: 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
# In case of geodatabase, prepend with:
# django.contrib.gis.db.backends.(postgis)
DATABASES = {
    # If you want to use another database, consider putting the database
    # settings in localsettings.py. Otherwise, if you change the settings in
    # the current file and commit them to the repository, other developers will
    # also use these settings whether they have that database or not.
    # One of those other developers is Jenkins, our continuous integration
    # solution. Jenkins can only run the tests of the current application when
    # the specified database exists. When the tests cannot run, Jenkins sees
    # that as an error.
    'default': {
        'NAME': '',      # via localsettings!
        'ENGINE': '',    # via localsettings!
        'USER': '',      # via localsettings!
        'PASSWORD': '',  # via localsettings!
        'HOST': '',      # via localsettings!
        'PORT': '',      # via localsettings!
        }
    }


try:
    # For local dev overrides.
    from ddsc_worker.localsettings import *  # NOQA
except ImportError:
    pass
