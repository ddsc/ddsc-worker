from django.core.management.base import BaseCommand
from ddsc_core.models import Timeseries
from ddsc_core.models.alarms import Alarm
from ddsc_core.models.alarms import Alarm_Item
from django.utils import timezone


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
            current_time = timezone.now()
            time_diff = current_time - alm.date_cr
            time_diff_sec = time_diff.days * 24 * 3600 + \
                time_diff.seconds + \
                time_diff.microseconds / 1000000
            alarm_or_not = []
            if (alm != [] and time_diff_sec > alm.frequency * 60):
                print 'executing, alarm name: %r' % alm.name
                logical_check = alm.logical_check
                print 'logical_check: %r' % logical_check
                for alm_itm in Alarm_Item.objects.filter(alarm_id=alm.id):
                    if alm_itm != []:
                        try:
                            ts_series = alm_itm.content_object.timeseries.all()
                        except:
                            ts_series = [alm_itm.content_object]
                        logical_check_item = alm_itm.logical_check
                        alarm_or_not_item = []
                        for ts in ts_series:
#                            time_diff = alm_itm.last_checked \
#                                - ts.latest_value_timestamp
#                            time_diff_sec = abs(time_diff.days * 24 * 3600) + \
#                                time_diff.seconds + \
#                                time_diff.microseconds / 1000000
                        # but I dont think microseconds is needed personally
#                            print 'time difference in seconds is: %r' \
#                                % time_diff_sec
                            print 'ts_lastest timestamp:'
                            print ts.latest_value_timestamp
                            print 'alarm_last_checked timestamp:'
                            print alm.date_cr
                            if ts.latest_value_timestamp > (alm.date_cr):
                                print 'checking timeseries: %r' % ts.uuid \
                                    + 'within alarm item id %r' % alm_itm.id
                                print 'comparison type is:' \
                                    + '%r' % alm_itm.comparision
                                print 'comparison value type is: ' \
                                    + '%r' % alm_itm.value_type
                                print 'comparison value is: ' \
                                    + '%r' % alm_itm.value_double
                                print 'timeseries value is: ' \
                                    + '%r' % ts.latest_value_number

                                if alm_itm.value_type == 1:
                                    alarm_or_not_item.append(
                                        compare(alm_itm.comparision,
                                          alm_itm.value_double,
                                          ts.latest_value_number)
                                    )
                                else:
                                    print 'To do: dealing with \
                                    non-waarde comparison!'
                            else:
                                print 'current alarm item has' \
                                    + 'already been checked'
                    if alarm_or_not_item != []:
                        ds = decision(alarm_or_not_item,
                            logical_check_item)
                        print 'alarm or not (item level)? ...: %r' \
                            % alarm_or_not_item
                        print 'alarm or not (item level) decision: ' \
                            + '%r' % ds
                        alarm_or_not.append(ds)
                alm.date_cr = timezone.now()
                alm.save()
            else:
                print 'current alarm has been checked!'
            if alarm_or_not != []:
                print '-------------------------------------------------------'
                print '-------------------------------------------------------'
                print 'alarm or not (alarm level)? ...: %r' % alarm_or_not
                print 'final alarm or not? \
                    %r' % decision(alarm_or_not, logical_check)
#            alm.active_status = False
#            alm.save()
            print 'finishing alarm name: %r  \n\n' % alm.name
        print 'completed~'
