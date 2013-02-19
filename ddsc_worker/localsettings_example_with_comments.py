from ddsc_worker.developmentsettings import * 
DATABASES = {
    'default': {
        'NAME': 'DDSC_DB',
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'USER': 'shaoqing',
        'PASSWORD': 'geoict',
        'HOST': 'localhost', # empty string for localhost.
        'PORT': '5432', # empty string for default.
    }
}
CASSANDRA = {
    'servers': [
        '10.10.101.118:9160',
#        'localhost:9160'
    ],
    'keyspace': 'speederKeySpace',
    'batch_size': 1000,
}


IMPORTER_PATH = {
    'ddsc_logging':
        '/home/shaoqing/gitrepo/ddsc-worker/var/log/ddsc_worker.log',
    'storage_base_path':
        '/home/shaoqing/ddsc/',
    'rejected_file':  # where to put the csv file when problem occurs
        'rejected_file/', # 
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
}

IMPORTER_GEOSERVER = {
    'geoserver_jar_pusher':  # where is the geotif_pub.jar file
        '/home/shaoqing/gitrepo/ddsc-worker/ddsc_worker/geotif_pub.jar',
    'geoserver_url':
        'http://10.10.101.118:8080/geoserver',
}
