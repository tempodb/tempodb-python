import unittest
import datetime
import json
from tempodb.client import Client, make_series_url
from tempodb.endpoint import BASE_URL, make_url_args
import tempodb.endpoint as p
from tempodb.protocol import Series, DataPoint
from monkey import monkeypatch_requests
from test_protocol_cursor import DummyResponse


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = Client('my_id', 'foo', 'bar')
        monkeypatch_requests(self.client.session)

    def test_make_series_url(self):
        key = 'foo'
        url = make_series_url(key)
        self.assertEquals(url, 'series/key/foo')

    def test_make_series_url_with_colon(self):
        key = 'foo:'
        url = make_series_url(key)
        self.assertEquals(url, 'series/key/foo%3A')

    def test_make_series_url_with_period(self):
        key = 'foo.'
        url = make_series_url(key)
        self.assertEquals(url, 'series/key/foo.')

    def test_client_constructor(self):
        self.assertEquals(self.client.session.base_url, BASE_URL)

    def test_create_series(self):
        resp_data = DummyResponse()
        self.client.session.pool.post.return_value = resp_data
        self.client.create_series('foo')
        self.client.session.pool.post.assert_called_once_with(
            'https://api.tempo-db.com/v1/series/',
            data='{"attributes": {}, "key": "foo", "tags": []}',
            auth=self.client.session.auth
        )

    def test_delete_series(self):
        resp_data = DummyResponse()
        self.client.session.pool.delete.return_value = resp_data

        key = 'bar'
        tag = ['baz']
        attr = {'abc': 'def'}
        allow_truncation = False

        params = {
            'key': key,
            'tag': tag,
            'attr': attr,
            'allow_truncation': str(allow_truncation).lower()
        }
        url_args = make_url_args(params)
        url = '?'.join([p.SERIES_ENDPOINT, url_args])
        self.client.delete_series(key='bar', tag=['baz'],
                                  attr={'abc': 'def'})
        self.client.session.pool.delete.assert_called_once_with(
            BASE_URL + url,
            auth=self.client.session.auth
        )

    def test_get_series(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "key": "foo",
            "id": "bar",
            "name": "",
            "tags": [],
            "attributes": {}
        })
        self.client.session.pool.get.return_value = resp_data
        r = self.client.get_series(key='foo')
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data.__class__.__name__, 'Series')

    def test_list_series(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps([{
            "key": "foo",
            "id": "bar",
            "name": "",
            "tags": [],
            "attributes": {}
        }])
        self.client.session.pool.get.return_value = resp_data
        r = self.client.list_series(key='foo')
        self.assertEquals(r.response.status, 200)
        self.assertEquals(len([a for a in r]), 1)
        self.client.session.pool.get.assert_called_once()

    def test_update_series(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "key": "foo",
            "id": "bar",
            "name": "",
            "tags": [],
            "attributes": {}
        })
        self.client.session.pool.put.return_value = resp_data
        series = Series({"key": "foo", "id": "bar", "name": "",
                         "tags": [], "attributes": {}}, None)
        r = self.client.update_series(series)
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data.__class__.__name__, 'Series')
        self.client.session.pool.put.assert_called_once()

    def test_read_data(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "data": [{
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            }],
            "tz": "UTC",
            "rollup": None
        })
        self.client.session.pool.get.return_value = resp_data
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        r = self.client.read_data('foo', start, end)
        self.assertEquals(r.response.status, 200)
        self.assertEquals(len([a for a in r]), 1)
        self.client.session.pool.get.assert_called_once()

    def test_aggregate_data(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "data": [{
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            }],
            "tz": "UTC",
            "aggregation": None
        })
        self.client.session.pool.get.return_value = resp_data
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        r = self.client.aggregate_data('sum', keys='foo', tags=['bar', 'baz'],
                                       start=start, end=end)
        self.assertEquals(r.response.status, 200)
        self.assertEquals(len([a for a in r]), 1)
        self.client.session.pool.get.assert_called_once()

    def test_find_data(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "data": [{
                "interval": {
                    "start": "2013-12-18T00:00:00",
                    "end": "2013-12-18T00:00:00",
                },
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            }],
            "tz": "UTC",
            "predicate": {
                "period": "1min",
                "function": "first"
            }
        })
        self.client.session.pool.get.return_value = resp_data
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        r = self.client.find_data('foo', start, end, 'max', '1min')
        self.assertEquals(r.response.status, 200)
        self.assertEquals(len([a for a in r]), 1)
        self.client.session.pool.get.assert_called_once()

    def test_write_data(self):
        test = [
            DataPoint.from_data(datetime.datetime.now(), 1.0),
            DataPoint.from_data('2012-01-08T00:21:54.000+0000', 1.0),
        ]

        resp_data = DummyResponse()
        self.client.session.pool.post.return_value = resp_data
        r = self.client.write_data('foo', test)
        self.client.session.pool.post.assert_called_once()
        self.assertEquals(r.status, 200)

    def test_write_multi(self):
        test = [
            DataPoint.from_data(datetime.datetime.now(), 1.0),
            DataPoint.from_data('2012-01-08T00:21:54.000+0000', 1.0),
        ]

        resp_data = DummyResponse()
        self.client.session.pool.post.return_value = resp_data
        r = self.client.write_multi(test)
        self.client.session.pool.post.assert_called_once()
        self.assertEquals(r.status, 200)

    def test_single_value(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "series": {
                "key": "foo",
                "id": "bar",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            },
        })
        self.client.session.pool.get.return_value = resp_data
        r = self.client.single_value('foo')
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data.__class__.__name__, 'SingleValue')
        self.client.session.pool.get.assert_called_once()

    def test_single_value_with_ts(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps({
            "series": {
                "key": "foo",
                "id": "bar",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            },
        })
        self.client.session.pool.get.return_value = resp_data
        r = self.client.single_value('foo', ts=datetime.datetime.now())
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data.__class__.__name__, 'SingleValue')
        self.client.session.pool.get.assert_called_once()

    def test_multi_series_single_value(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps([{
            "series": {
                "key": "foo",
                "id": "bar",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            },
        }])
        self.client.session.pool.get.return_value = resp_data
        r = self.client.multi_series_single_value('foo')
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data[0].__class__.__name__, 'SingleValue')
        self.assertEquals(len(r.data), 1)
        self.client.session.pool.get.assert_called_once()

    def test_multi_series_single_value_with_ts(self):
        resp_data = DummyResponse()
        resp_data.text = json.dumps([{
            "series": {
                "key": "foo",
                "id": "bar",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {
                "t": "2013-12-18T00:00:00",
                "v": "bar",
            },
        }])
        self.client.session.pool.get.return_value = resp_data
        r = self.client.multi_series_single_value('foo',
                                                  ts=datetime.datetime.now())
        self.assertEquals(r.status, 200)
        self.assertEquals(r.data[0].__class__.__name__, 'SingleValue')
        self.assertEquals(len(r.data), 1)
        self.client.session.pool.get.assert_called_once()

    def test_delete(self):
        resp_data = DummyResponse()
        self.client.session.pool.delete.return_value = resp_data
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        r = self.client.delete('foo', start, end)
        self.assertEquals(r.status, 200)
        self.client.session.pool.delete.assert_called_once()
