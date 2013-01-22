from setuptools import setup

version = '0.1dev'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'Django',
    'celery >= 3.0.12',
    'ddsc-core',
    'django-celery',
    'django-extensions',
    'django-nose',
    'gunicorn',
    'lizard-ui',
    'python-memcached',
    'raven',
    'tslib',
    'werkzeug',
    ],

setup(name='ddsc-worker',
      version=version,
      description="TODO",
      long_description=long_description,
      # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=['Programming Language :: Python',
                   'Framework :: Django',
                   ],
      keywords=[],
      author='TODO',
      author_email='TODO@nelen-schuurmans.nl',
      url='',
      license='GPL',
      packages=['ddsc_worker'],
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      entry_points={
          'console_scripts': [
          'call_by_incron_forked = ddsc_worker.call_by_incron_forked:main',
          ]},
      )
