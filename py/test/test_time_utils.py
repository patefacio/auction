from auction.time_utils import start_of_date, timestamp, \
    datetime_from_timestamp, chicago_time, UTC_TZ, NY_TZ, CHI_TZ
from datetime import datetime, date

def test_start_of_date():
    sod = start_of_date(2011, 7, 22, CHI_TZ)
    dt = datetime_from_timestamp(sod)
    assert(dt.microsecond == 0)
    assert(dt.second == 0)
    assert(dt.minute == 0)
    assert(dt.year == 2011)
    assert(dt.month == 7)
    assert(dt.day == 22)
    assert(dt.tzinfo == UTC_TZ)
    # not testing our code, just illustrating any datetime converted to a
    # different timezone is still equivalent to the original
    assert(dt == dt.astimezone(NY_TZ))
    # chicago start of day, stored as UTC, converted to chicago time
    assert(chicago_time(sod) == dt)

def test_timestamp():
    # Get a current timestamp
    ts = timestamp()
    dt = datetime_from_timestamp(ts)
    now = datetime.utcnow().replace(tzinfo=UTC_TZ)
    today = date.today()
    assert(dt.tzinfo == UTC_TZ)
    assert(dt.year == today.year)
    assert(dt.month == today.month)
    delta = now-dt
    assert(delta.microseconds > 0)
    assert(delta.seconds == 0)

