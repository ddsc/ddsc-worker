from django.core.management.base import BaseCommand
from django.template import Context
from django.template import Template
from datetime import datetime
from ddsc_core.models import Timeseries
from ddsc_core.models.alarms import Alarm
from ddsc_core.models.alarms import Alarm_Item
from django.utils import timezone
import smtplib


SMTP_HOST = '10.10.101.21'  # To be decided where to put in the settings file
SMTP_PORT = 25
FROM_ADDRESS = 'no_reply@dijkdata.nl'


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
            final_decision = False
            list_ts_info = ''
            current_time = timezone.now()
            time_diff = current_time - alm.last_checked
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
                            print 'ts_lastest timestamp:'
                            print ts.latest_value_timestamp
                            print 'alarm_last_checked timestamp:'
                            print alm.last_checked
                            if ts.latest_value_timestamp > (alm.last_checked):
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
                                list_ts_info += 'Timeseries: ' + ts.uuid + \
                                    ' \t' + \
                                    'at ' + ts.latest_value_timestamp.\
                                    strftime('%Y-%m-%d %H:%M:%S+01') + ' \t' +\
                                    'value:' + str(ts.latest_value_number) +\
                                     ' \t' + 'threshold:' + \
                                     str(alm_itm.value_double) + '\n'
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
                alm.last_checked = timezone.now()
                alm.save()
            else:
                print 'current alarm has been checked!'
            if alarm_or_not != []:
                print '-------------------------------------------------------'
                print '-------------------------------------------------------'
                print 'alarm or not (alarm level)? ...: %r' % alarm_or_not
                final_decision = decision(alarm_or_not, logical_check)
                print 'final alarm or not? \
                    %r' % final_decision
                if final_decision is True:
                    smtp = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
                    from_addr = FROM_ADDRESS
                    t = Template(alm.template)
                    CONTEXT_DICT = {
                        'alarm_name': alm.name,
                        'urgency': alm.urgency,
                        'alarming_time': datetime.now(),
                        'list_timeseries': list_ts_info,
                        'sender_name': alm.content_object,
                    }
                    c = Context(CONTEXT_DICT)
                    msg = t.render(c)
                    try:
                        user_list = alm.content_object.members.all()
                        for user in user_list:
                            to_addr = user.email
                            header = 'To:' + to_addr + '\n' + 'From: ' + \
                                from_addr + '\n' + 'Subject: ALARM! \n'
                            msg = header + msg
                            smtp.sendmail(from_addr, to_addr, msg)
                    except:
                        user = alm.content_object
                        to_addr = user.email
                        header = 'To:' + to_addr + '\n' + 'From: ' + \
                                from_addr + '\n' + 'Subject: ALARM! \n'
                        msg = header + msg
                        smtp.sendmail(from_addr, to_addr, msg)
#            alm.active_status = False
#            alm.save()
            print 'finishing alarm name: %r  \n\n' % alm.name
        print 'completed~'
