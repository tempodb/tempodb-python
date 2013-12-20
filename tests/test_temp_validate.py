import unittest
import datetime
from tempodb.temporal.validate import check_time_param, convert_iso_stamp


class TestTempValidate(unittest.TestCase):
    def setUp(self):
        pass

    def test_check_time_param_valid_string(self):
        s = '2008-01-03T10:12:32.231+0000'
        ret = check_time_param(s)
        self.assertEquals(ret, s)

    def test_check_time_param_valid_string2(self):
        s = '2008-01-03T10:12:32.231321+0000'
        ret = check_time_param(s)
        self.assertEquals(ret, s)

    def test_check_time_param_valid_string3(self):
        s = '2008-01-03T10:12:32.231321+05:00'
        ret = check_time_param(s)
        self.assertEquals(ret, s)

    def test_check_time_param_invalid_string(self):
        s = '20-01-03T10:12:32.231321+0000'
        self.assertRaises(ValueError, check_time_param, s)

    def test_check_time_param_invalid_string2(self):
        s = '2012-01/03T10:1:32.231321+0000'
        self.assertRaises(ValueError, check_time_param, s)

    def test_check_time_param_datetime_obj(self):
        dt = datetime.datetime(2013, 1, 1, 10, 12, 15)
        s = '2013-01-01T10:12:15'
        ret = check_time_param(dt)
        self.assertEquals(ret, s)

    def test_convert_iso_param(self):
        s = '2013-01-01T10:12:15'
        ret = convert_iso_stamp(s)
        self.assertEquals(ret.year, 2013)
        self.assertEquals(ret.month, 1)
        self.assertEquals(ret.day, 1)
        self.assertEquals(ret.hour, 10)
        self.assertEquals(ret.minute, 12)
        self.assertEquals(ret.second, 15)

    def test_convert_iso_param2(self):
        s = '2013-01-01T10:12:15.032+0000'
        ret = convert_iso_stamp(s)
        self.assertEquals(ret.year, 2013)
        self.assertEquals(ret.month, 1)
        self.assertEquals(ret.day, 1)
        self.assertEquals(ret.hour, 10)
        self.assertEquals(ret.minute, 12)
        self.assertEquals(ret.second, 15)
        self.assertEquals(ret.microsecond, 32000)

    def test_convert_iso_param_with_tz(self):
        s = '2013-01-01T10:12:15.032'
        ret = convert_iso_stamp(s, tz='UTC')
        self.assertEquals(ret.year, 2013)
        self.assertEquals(ret.month, 1)
        self.assertEquals(ret.day, 1)
        self.assertEquals(ret.hour, 10)
        self.assertEquals(ret.minute, 12)
        self.assertEquals(ret.second, 15)
        self.assertEquals(ret.microsecond, 32000)

    def test_convert_iso_param_with_tz2(self):
        s = '2013-01-01T10:12:15.032'
        ret = convert_iso_stamp(s, tz='US/Eastern')
        self.assertEquals(ret.year, 2013)
        self.assertEquals(ret.month, 1)
        self.assertEquals(ret.day, 1)
        self.assertEquals(ret.hour, 10)
        self.assertEquals(ret.minute, 12)
        self.assertEquals(ret.second, 15)
        self.assertEquals(ret.microsecond, 32000)
