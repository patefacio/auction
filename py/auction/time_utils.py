from datetime import datetime
from dateutil import tz
import time
import calendar


UTC_TZ = tz.gettz('UTC')
NY_TZ = tz.gettz('America/New_York')
CHI_TZ = tz.gettz('America/Chicago')
LOCAL_TZ = tz.tzlocal()
__SUBSECOND_RESOLUTION__ = 1000000

def start_of_date(year, month, day, tzinfo):
    """
    Given date information and timezone, create a timestamp
    """
    result = datetime(year, month, day, tzinfo=tzinfo).astimezone(UTC_TZ)
    return timestamp_from_datetime(result)

def timestamp():
    """
    Get a current timestamp
    """
    now = datetime.utcnow()
    return calendar.timegm(now.utctimetuple())*__SUBSECOND_RESOLUTION__ + now.microsecond

def datetime_from_timestamp(ts):
    """
    Given a timestamp, create the corresponding datetime object
    """
    return datetime.fromtimestamp(float(ts)/__SUBSECOND_RESOLUTION__, UTC_TZ)

def timestamp_from_datetime(dt):
    """
    Given a datetime in utc, create the corresponding timestamp
    """
    return calendar.timegm(dt.utctimetuple())*__SUBSECOND_RESOLUTION__ + dt.microsecond

def timestamp_from_mtime(mt):
    """
    Given a file mtime, create the corresponding timestamp
    """
    return int(mt*__SUBSECOND_RESOLUTION__)

def chicago_time(ts):
    """
    Given a timestamp (as utc), get the corresponding chicago time
    """
    stamp = datetime.fromtimestamp(float(ts)/__SUBSECOND_RESOLUTION__, UTC_TZ)
    return stamp.astimezone(CHI_TZ)

def chicago_time_str(ts):
    return chicago_time(ts).strftime('%H:%M:%S:%f')

if __name__ == "__main__":


    def make_timestamp(start_of_date, seconds, millis):
        seconds = int(seconds)
        millis = int(millis)
        return start_of_date + seconds*1000000 + millis*1000

    def grab_time():
        for i in range(10000):
            ts = timestamp()

    import sys
    now = timestamp()
    print timestamp(), timestamp(), timestamp()
    print "now is ", datetime_from_timestamp(now)
    print "chicago time is ", chicago_time(now).strftime('%H:%M:%S:%f')
    for i in range(20):
        t = now + i * (60*60*24*356*__SUBSECOND_RESOLUTION__)
        print "chicago time:", chicago_time(t), " => ", int(t), " vs ", sys.maxint

    grab_time()

    print chicago_time(1311325200095000)

#    sod = timestamp_from_datetime(datetime(2011, 7, 22))
    print "CH sod", start_of_date(2011, 7, 22, CHI_TZ)
    print "NY sod", start_of_date(2011, 7, 22, NY_TZ)

    print "CH", chicago_time(start_of_date(2011, 7, 22, CHI_TZ))
    print "NY", chicago_time(start_of_date(2011, 7, 22, NY_TZ))

    dt = datetime.utcnow()
    print (calendar.timegm(dt.utctimetuple())*__SUBSECOND_RESOLUTION__), "vs", dt.microsecond
    print (calendar.timegm(dt.utctimetuple())*__SUBSECOND_RESOLUTION__ + dt.microsecond), "vs", dt.microsecond

    print chicago_time(1311321600730000)
