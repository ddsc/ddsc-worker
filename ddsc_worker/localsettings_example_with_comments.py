DATABASES = {
    'default': {
        'NAME': 'ddsc',
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'USER': 'xxxx',
        'PASSWORD': 'xxxxxxxx',
        'HOST': 'xx.xx.xxx.xxx',
        'PORT': '',
    }
}

CASSANDRA = {
    'servers': [
        'xxx.xxx.xxx.xx:9160',
        'xx.xxx.xxx.xx:9160',
        'xx.xxx.xxx.xxx:9160',
    ],
    'keyspace': 'ddsc',
    'batch_size': 1000,
}

IMPORTER_PATH = {
    'storage_base_path':
        '/mnt/file/',
    'rejected_file':  # where to put the csv file when problem occurs
        'rejected_csv/', #
    'image':  # where to put the image data, path will be created automatically
        'images/',
    'geotiff':  # where to put the rs image data, path will be created automatically
        'geo_tiff/',
    'video':  # where to put the video data, path will be created automatically
        'video/',
    'pdf':  # where to put the pdf file, path will be created automatically
        'pdf/',
    'unrecognized':  # where to put unrecognizable type of file, path will be created automatically
        'unknown/',
    'lmw':
        '/mnt/ftp/lmw_ddsc/',
}

SMTP = {
    'host':
        '10.10.10.110',
    'port':
        25,
    'sender':
        'no_reply@dijkdata.nl',
}
