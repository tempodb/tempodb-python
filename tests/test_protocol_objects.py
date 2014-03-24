import unittest
import datetime
import json
from tempodb.protocol.objects import JSONSerializable
from tempodb.protocol.objects import DataPoint, MultiPoint, DataPointFound
from tempodb.protocol.objects import SingleValue, SeriesSummary


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

    def test_data_point_found_to_dictionary(self):
        d = {'found': {'t': '2013-12-18T00:00:00', 'v': 1.0},
             'interval': {'start': '2013-12-01T00:00:00',
                          'end': '2013-12-31T23:59:59'}}
        d = DataPointFound(d, None)
        j = d.to_dictionary()
        self.assertEquals(j['found']['t'], '2013-12-18T00:00:00')
        self.assertEquals(j['found']['v'], 1.0)
        self.assertEquals(j['interval']['start'], '2013-12-01T00:00:00')
        self.assertEquals(j['interval']['end'], '2013-12-31T23:59:59')

    def test_data_point_found_to_dictionary_with_tz(self):
        d = {'found': {'t': '2013-12-18T00:00:00', 'v': 1.0},
             'interval': {'start': '2013-12-01T00:00:00',
                          'end': '2013-12-31T23:59:59'}}
        d = DataPointFound(d, None, tz='US/Eastern')
        j = d.to_dictionary()
        self.assertEquals(j['found']['t'], '2013-12-18T00:00:00-05:00')
        self.assertEquals(j['found']['v'], 1.0)
        self.assertEquals(j['interval']['start'], '2013-12-01T00:00:00-05:00')
        self.assertEquals(j['interval']['end'], '2013-12-31T23:59:59-05:00')

    def test_data_point_found_to_json(self):
        d = {'found': {'t': '2013-12-18T00:00:00', 'v': 1.0},
             'interval': {'start': '2013-12-01T00:00:00',
                          'end': '2013-12-31T23:59:59'}}
        df = DataPointFound(d, None)
        j = df.to_json()
        self.assertEquals(j, json.dumps(d))

    def test_multi_point_with_tz(self):
        d = {'t': '2013-12-18T00:00:00', 'v': {'foo': 1.0, 'bar': 3.0}}
        mp = MultiPoint(d, None, tz='US/Eastern')
        self.assertEquals(mp.t.isoformat(), '2013-12-18T00:00:00-05:00')

    def test_multi_point_get_valid_key(self):
        d = {'t': '2013-12-18T00:00:00', 'v': {'foo': 1.0, 'bar': 3.0}}
        mp = MultiPoint(d, None, tz='US/Eastern')
        self.assertEquals(mp.get('foo'), 1.0)

    def test_multi_point_get_invalid_key(self):
        d = {'t': '2013-12-18T00:00:00', 'v': {'foo': 1.0, 'bar': 3.0}}
        mp = MultiPoint(d, None, tz='US/Eastern')
        self.assertEquals(mp.get('baz'), None)

    def test_single_value(self):
        d = {"data": {
                "t": "2013-12-31T23:00:00.000Z",
                "v": 35.8872769971233
            },
            "series": {
                "attributes": {
                    "project": "perftest1"
                },
                "id": "fake",
                "key": "foo",
                "name": "",
                "tags": [
                    "subset"
                ]
            }
        }
        sv = SingleValue(d, None)
        self.assertEquals(sv.data.t.month, 12)
        self.assertEquals(sv.series.key, 'foo')

    def test_single_value_to_dictionary(self):
        d = {"data": {
                "t": "2013-12-31T23:00:00.000Z",
                "v": 35.8872769971233
            },
            "series": {
                "attributes": {
                    "project": "perftest1"
                },
                "id": "fake",
                "key": "foo",
                "name": "",
                "tags": [
                    "subset"
                ]
            }
        }
        sv = SingleValue(d, None)
        j = sv.to_dictionary()
        self.assertEquals(j['data']['t'], '2013-12-31T23:00:00+00:00')
        self.assertEquals(j['series']['key'], 'foo')

    def test_single_value_to_json(self):
        d = {"data": {
                "t": "2013-12-31T23:00:00.000Z",
                "v": 35.8872769971233
            },
            "series": {
                "attributes": {
                    "project": "perftest1"
                },
                "id": "fake",
                "key": "foo",
                "name": "",
                "tags": [
                    "subset"
                ]
            }
        }
        sv = SingleValue(d, None)
        j = sv.to_json()
        dj = json.loads(j)
        #these are serialized differently or not at all
        del d['series']['id']
        d['data']['t'] = '2013-12-31T23:00:00+00:00'
        d1 = json.loads(json.dumps(d))
        self.assertDictEqual(dj, d1)

    def test_series_summary(self):
        d = {"series":
                {"id": "foo",
                 "key": "stuff",
                 "name": "",
                 "tags": [],
                 "attributes": {}},
             "tz": "UTC",
             "end": "2012-01-02T00:00:00.000Z",
             "start": "2012-01-01T00:00:00.000Z",
             "summary":
                {"count": 1440,
                 "mean": 24.81055812507774,
                 "min": 0.05106735242182414,
                 "max": 49.96047239524747,
                 "stddev": 14.518747713268956,
                 "sum": 35727.203700111946}
             }
        ss = SeriesSummary(d, None)
        self.assertEquals(ss.series.key, 'stuff')
        self.assertEquals(ss.summary.count, 1440)

    def test_series_summary_to_dictionary(self):
        d = {"series":
                {"id": "foo",
                 "key": "stuff",
                 "name": "",
                 "tags": [],
                 "attributes": {}},
             "tz": "UTC",
             "end": "2012-01-02T00:00:00.000Z",
             "start": "2012-01-01T00:00:00.000Z",
             "summary":
                {"count": 1440,
                 "mean": 24.81055812507774,
                 "min": 0.05106735242182414,
                 "max": 49.96047239524747,
                 "stddev": 14.518747713268956,
                 "sum": 35727.203700111946}
             }
        ss = SeriesSummary(d, None)
        di = ss.to_dictionary()
        self.assertEquals(di['series']['key'], 'stuff')
        self.assertEquals(di['summary']['count'], 1440)

    def test_series_summary_to_json(self):
        d = {"series":
                {"id": "foo",
                 "key": "stuff",
                 "name": "",
                 "tags": [],
                 "attributes": {}},
             "tz": "UTC",
             "end": "2012-01-02T00:00:00.000Z",
             "start": "2012-01-01T00:00:00.000Z",
             "summary":
                {"count": 1440,
                 "mean": 24.81055812507774,
                 "min": 0.05106735242182414,
                 "max": 49.96047239524747,
                 "stddev": 14.518747713268956,
                 "sum": 35727.203700111946}
             }
        ss = SeriesSummary(d, None)
        j = ss.to_json()
        dj = json.loads(j)
        d['start'] = '2012-01-01T00:00:00+00:00'
        d['end'] = '2012-01-02T00:00:00+00:00'
        del d['series']['id']
        self.maxDiff = None
        self.assertDictEqual(dj, d)
