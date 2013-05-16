from os import listdir
from os.path import isfile
from django.core.management.base import BaseCommand
from ddsc_worker.celery import celery 


class Command(BaseCommand):
    help = 'Scan a file directory to reschedule the tasks'
    def handle(self, *args, **options):
        filePath_file = args[0]
        filePath_socket = args[1]
        
        for file in listdir(filePath_file):
            if isfile(filePath_file + file):
                celery.send_task(
                    "ddsc_worker.tasks.new_file_detected", kwargs={
                    "pathDir": filePath_file, 
                    "fileName": file,
                    }
                )
                
        for file in listdir(filePath_socket):
            if isfile(filePath_socket + file):
                celery.send_task(
                    "ddsc_worker.tasks.new_socket_detected", kwargs={
                    "pathDir": filePath_socket, 
                    "fileName": file,
                    }
                )