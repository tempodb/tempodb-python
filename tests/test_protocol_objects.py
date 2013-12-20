import unittest
import datetime
import json
from tempodb.protocol.objects import JSONSerializable
from tempodb.protocol.objects import DataPoint


class TestProtocolObjects(unittest.TestCase):
    def test_json_serializable_constructor_with_text(self):
        JSONSerializable.properties = ['foo']
        json_text = json.dumps({'foo': 'bar'})
        o = JSONSerializable(json_text, None)
        JSONSerializable.properties = []
        self.assertEquals(o.foo, 'bar')

    def test_json_serializable_constructor_with_obj(self):
        JSONSerializable.properties = ['foo']
        json_text = {'foo': 'bar'}
        o = JSONSerializable(json_text, None)
        JSONSerializable.properties = []
        self.assertEquals(o.foo, 'bar')

    def test_json_serializable_constructor_with_invalid_obj(self):
        JSONSerializable.properties = ['baz']
        json_text = {'foo': 'bar'}
        self.assertRaises(ValueError, JSONSerializable, json_text,
                          None)
        JSONSerializable.properties = []

    def test_json_serializable_to_json(self):
        JSONSerializable.properties = ['foo']
        json_text = {'foo': 'bar'}
        o = JSONSerializable(json_text, None)
        j = o.to_json()
        self.assertEquals(j, json.dumps(json_text))
        JSONSerializable.properties = []

    def test_json_serializable_to_dict(self):
        JSONSerializable.properties = ['foo']
        json_text = {'foo': 'bar'}
        o = JSONSerializable(json_text, None)
        j = o.to_dictionary()
        self.assertEquals(j, json_text)
        JSONSerializable.properties = []

    def test_data_point_from_data_valid(self):
        t = datetime.datetime.now()
        v = 1.0
        series_id = 'foo'
        key = 'bar'

        d = DataPoint.from_data(t, v, series_id, key)
        self.assertEquals(d.v, 1.0)
        self.assertEquals(d.id, 'foo')
        self.assertEquals(d.key, 'bar')

    def test_data_point_from_data_with_tz(self):
        t = '2013-12-18T00:00:00'
        v = 1.0
        series_id = 'foo'
        key = 'bar'

        d = DataPoint.from_data(t, v, series_id, key, tz='US/Eastern')
        j = d.to_dictionary()
        self.assertEquals(j['t'], '2013-12-18T00:00:00-05:00')
        self.assertEquals(d.v, 1.0)
        self.assertEquals(d.id, 'foo')
        self.assertEquals(d.key, 'bar')

    def test_data_point_from_data_invalid(self):
        t = datetime.datetime.now()
        v = '1.0aas'
        series_id = 'foo'
        key = 'bar'

        self.assertRaises(ValueError, DataPoint.from_data, t, v,
                          series_id, key)

    def test_data_point_from_json(self):
        d = {'t': '1', 'v': 1.0}
        d = DataPoint(d, None)
        self.assertEquals(d.v, 1.0)

    def test_data_point_from_json_with_optional_params(self):
        d = {'t': '1', 'v': 1.0, 'key': 'foo', 'id': 'bar'}
        d = DataPoint(d, None)
        self.assertEquals(d.v, 1.0)
        self.assertEquals(d.key, 'foo')
        self.assertEquals(d.id, 'bar')

    def test_data_point_to_json(self):
        d = {'t': '2013-12-18T00:00:00', 'v': 1.0}
        d = DataPoint(d, None)
        j = json.loads(d.to_json())
        self.assertEquals(j['t'], '2013-12-18T00:00:00')
        self.assertEquals(j['v'], 1.0)

    def test_data_point_to_json_with_tz(self):
        d = {'t': '2013-12-18T00:00:00', 'v': 1.0}
        d = DataPoint(d, None, tz='US/Eastern')
        j = json.loads(d.to_json())
        self.assertEquals(j['t'], '2013-12-18T00:00:00-05:00')
        self.assertEquals(j['v'], 1.0)

    def test_data_point_to_json_with_optional_params(self):
        d = {'t': '2013-12-18T00:00:00',
             'v': 1.0, 'key': 'foo', 'id': 'bar'}
        d = DataPoint(d, None)
        j = json.loads(d.to_json())
        self.assertEquals(j['t'], '2013-12-18T00:00:00')
        self.assertEquals(j['v'], 1.0)
        self.assertEquals(j['key'], 'foo')
        self.assertEquals(j['id'], 'bar')

    def test_data_point_to_dictionary(self):
        d = {'t': '2013-12-18T00:00:00', 'v': 1.0}
        d = DataPoint(d, None)
        j = d.to_dictionary()
        self.assertEquals(j['t'], '2013-12-18T00:00:00')
        self.assertEquals(j['v'], 1.0)

    def test_data_point_to_dictionary_with_tz(self):
        d = {'t': '2013-12-18T00:00:00', 'v': 1.0}
        d = DataPoint(d, None, tz='US/Eastern')
        j = d.to_dictionary()
        self.assertEquals(j['t'], '2013-12-18T00:00:00-05:00')
        self.assertEquals(j['v'], 1.0)

    def test_data_point_to_dictionary_with_optional_params(self):
        d = {'t': '2013-12-18T00:00:00',
             'v': 1.0, 'key': 'foo', 'id': 'bar'}
        d = DataPoint(d, None)
        j = d.to_dictionary()
        self.assertEquals(j['t'], '2013-12-18T00:00:00')
        self.assertEquals(j['v'], 1.0)
        self.assertEquals(j['key'], 'foo')
        self.assertEquals(j['id'], 'bar')
