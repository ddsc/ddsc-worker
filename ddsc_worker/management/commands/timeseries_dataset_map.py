from django.core.management.base import BaseCommand
from ddsc_core.models import Timeseries


#class Command(BaseCommand):
#    def handle(self):
for ts in Timeseries.objects.all():
    ts.data_set = [1]
