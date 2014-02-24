import re
import dateutil.parser
import pytz


ISO = re.compile(
    r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    '((.\d{3,6})?([+-](\d{4|\d{2}:\d{2}}))?)?')


def check_time_param(t):
    """Check whether a string sent in matches the ISO8601 format.  If a
    Datetime object is passed instead, it will be converted into an ISO8601
    compliant string.

    :param t: the datetime to check
    :type t: string or Datetime
    :rtype: string"""

    if type(t) is str:
        if not ISO.match(t):
            raise ValueError('Date string "%s" does not match ISO8601 format' %
                             (t))
        return t
    else:
        return t.isoformat()


def convert_iso_stamp(t, tz=None):
    """Convert a string in ISO8601 form into a Datetime object.  This is mainly
    used for converting timestamps sent from the TempoDB API, which are
    assumed to be correct.

    :param string t: the timestamp to convert
    :rtype: Datetime object"""

    if t is None:
        return None

    dt = dateutil.parser.parse(t)
    if tz is not None:
        timezone = pytz.timezone(tz)
        if dt.tzinfo is None:
            dt = timezone.localize(dt)
    return dt
