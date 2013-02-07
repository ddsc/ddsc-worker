from django.core.management.base import BaseCommand
from ddsc_core.models import Timeseries
from ddsc_core.models.alarms import Alarm
from ddsc_core.models.alarms import Alarm_Item


def compare(x, y, z):
    return {
        1: y == z,
        2: y != z,
        3: y > z,
        4: y < z,
    }.get(x, 'error')


def decision(x, y):
    return {
        0: sum(x) == len(x),
        1: sum(x) != 0,
    }.get(y, 'error')


class Command(BaseCommand):
    args = '<none file>'
    help = 'trigger the amazing alarms'

    def handle(self, *args, **options):
        for alm in Alarm.objects.filter(active_status=True):
            alarm_or_not = []
            if alm != []:
                print 'executing, alarm name: %r' % alm.name
                logical_check = alm.logical_check
                print 'logical_check: %r' % logical_check
                for alm_itm in Alarm_Item.objects.filter(alarm_id=alm.id):
                    if alm_itm != []:
                        ts = alm_itm.timeseries
                        print 'checking timeseries: %r within \
                        alarm item id %r' % (ts.name, alm_itm.id)
                        if (alm_itm.last_checked
                            - ts.latest_value_timestamp).days < 0:
                            print 'checking alarm items id: %r' % alm_itm.id
                            print 'comparasion type is\
                             %r' % alm_itm.comparision
                            print 'comparasion value type is\
                             %r' % alm_itm.value_type
                            print 'comparasion value is\
                             %r' % alm_itm.value_double
                            print 'timeseries value is\
                             %r' % ts.latest_value_number

                            alm_itm.last_checked = ts.latest_value_timestamp
                            alm_itm.save()

                            if alm_itm.value_type == 1:
                                alarm_or_not.append(
                                    compare(alm_itm.comparision,
                                      alm_itm.value_double,
                                      ts.latest_value_number)
                                )
                            else:
                                print 'To do: dealling with \
                                non-waarde comparision!'

                        else:
                            print 'current alarm item has already been checked'
            if alarm_or_not != []:
                print 'alarm or not? ...: %r' % alarm_or_not
                print 'final alarm or not? \
                    %r' % decision(alarm_or_not, logical_check)
            alm.active_status = False
            alm.save()
            print 'finishing alarm name: %r  \n\n' % alm.name
        print 'completed~'