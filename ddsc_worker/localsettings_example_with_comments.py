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
SOCKS = {
    'host': '10.10.101.118',  # socket server hosts
    'port': 5008,
    'time_per_csv': 10,  # every 1 minute forms a csv file
    'socket_dst':
        '/home/shaoqing/testdata/socket/',
}

IMPORTER = {
    'rejected_file':  # where to put the csv file when problem occurs
        '/tmp/alex/dst/rejected_file/', # 
    'image':  # where to put the image data, path will be created automatically
        '/home/alex/dst/images/',
    'geotiff':  # where to put the rs image data, path will be created automatically
        '/home/alex/dst/geo_tiff/',
    'video':  # where to put the video data, path will be created automatically
        '/home/alex/dst/video/',
    'pdf':  # where to put the pdf file, path will be created automatically
        '/home/alex/dst/pdf/',
    'unrecognized':  # where to put unrecognizable type of file, path will be created automatically
        '/tmp/alex/dst/unknown/',
    'geoserver_jar_pusher':  # where is the geotif_pub.jar file
        '/home/shaoqing/gitrepo/ddsc-worker/ddsc_worker/geotif_pub.jar',
    'geoserver_url':
        'http://10.10.101.118:8080/geoserver',
}

LOGGING_DST = {
    'ddsc_logging':  # where to put importer logging file, path has to be existed
        '/home/shaoqing/gitrepo/ddsc-worker/var/log/ddsc.log',
    'socks_logging':
        '/home/shaoqing/gitrepo/ddsc-worker/var/log/socks.log',  # where to put socket logging file, path has to be existed
}

