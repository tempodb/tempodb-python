#!/usr/bin/env python
# encoding: utf-8

import datetime
import mock
import requests
import simplejson
from unittest2 import TestCase

import tempodb
from tempodb import Client, DataPoint, DataSet, Series, Summary


class MockResponse(object):

    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


class ClientTest(TestCase):

    def setUp(self):
        self.client = Client('key', 'secret', 'example.com', 443, True)
        self.get_headers = {
            'User-Agent': 'tempodb-python/%s' % tempodb.get_version()
        }
        self.put_headers = {
            'User-Agent': 'tempodb-python/%s' % tempodb.get_version(),
            'Content-Type': 'application/json',
        }
        self.post_headers = self.put_headers

    def test_init(self):
        client = Client('key', 'secret', 'example.com', 80, False)
        self.assertEqual(client.key, 'key')
        self.assertEqual(client.secret, 'secret')
        self.assertEqual(client.host, 'example.com')
        self.assertEqual(client.port, 80)
        self.assertEqual(client.secure, False)

    def test_defaults(self):
        client = Client('key', 'secret')
        self.assertEqual(client.host, 'api.tempo-db.com')
        self.assertEqual(client.port, 443)
        self.assertEqual(client.secure, True)

    @mock.patch('requests.get')
    def test_get_series(self, requests_get):
        requests_get.return_value = MockResponse(200, """[{
            "id": "id",
            "key": "key",
            "name": "name",
            "tags": ["tag1", "tag2"],
            "attributes": {"key1": "value1"}
        }]""")

        series = self.client.get_series()
        requests_get.assert_called_once_with(
            'https://example.com:443/v1/series/',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        expected = [Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])]
        self.assertEqual(series, expected)

    @mock.patch('requests.post')
    def test_create_series(self, requests_post):
        requests_post.return_value = MockResponse(200, """{
            "id": "id",
            "key": "my-key.tag1.1",
            "name": "",
            "tags": ["my-key", "tag1"],
            "attributes": {}
        }""")
        series = self.client.create_series("my-key.tag1.1")

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/series/',
            data="""{"key": "my-key.tag1.1"}""",
            auth=('key', 'secret'),
            headers=self.post_headers
        )
        expected = Series('id', 'my-key.tag1.1', '', {}, ['my-key', 'tag1'])
        self.assertEqual(series, expected)

    @mock.patch('requests.post')
    def test_create_series_validity_error(self, requests_post):
        with self.assertRaises(ValueError):
            series = self.client.create_series('key.b%^.test')

    @mock.patch('requests.put')
    def test_update_series(self, requests_put):
        update = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1'])
        requests_put.return_value = MockResponse(200, simplejson.dumps(update.to_json()))

        updated = self.client.update_series(update)

        requests_put.assert_called_once_with(
            'https://example.com:443/v1/series/id/id/',
            auth=('key', 'secret'),
            data=simplejson.dumps(update.to_json()),
            headers=self.put_headers
        )
        self.assertEqual(update, updated)

    @mock.patch('requests.get')
    def test_read_id(self, requests_get):
        requests_get.return_value = MockResponse(200, """{
            "series": {
                "id": "id",
                "key": "key",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "start": "2012-03-27T00:00:00.000",
            "end": "2012-03-28T00:00:00.000",
            "data": [{"t": "2012-03-27T00:00:00.000", "v": 12.34}],
            "summary": {}
        }""")

        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        dataset = self.client.read_id('id', start, end)

        expected = DataSet(Series('id', 'key'), start, end, [DataPoint(start, 12.34)], Summary())
        requests_get.assert_called_once_with(
            'https://example.com:443/v1/series/id/id/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    @mock.patch('requests.get')
    def test_read_key(self, requests_get):
        requests_get.return_value = MockResponse(200, """{
            "series": {
                "id": "id",
                "key": "key1",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "start": "2012-03-27T00:00:00.000",
            "end": "2012-03-28T00:00:00.000",
            "data": [{"t": "2012-03-27T00:00:00.000", "v": 12.34}],
            "summary": {}
        }""")

        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        dataset = self.client.read_key('key1', start, end)

        expected = DataSet(Series('id', 'key1'), start, end, [DataPoint(start, 12.34)], Summary())
        requests_get.assert_called_once_with(
            'https://example.com:443/v1/series/key/key1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    @mock.patch('requests.get')
    def test_read(self, requests_get):
        requests_get.return_value = MockResponse(200, """[{
            "series": {
                "id": "id",
                "key": "key1",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "start": "2012-03-27T00:00:00.000",
            "end": "2012-03-28T00:00:00.000",
            "data": [{"t": "2012-03-27T00:00:00.000", "v": 12.34}],
            "summary": {}
        }]""")

        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        datasets = self.client.read(start, end, keys=['key1'])

        expected = [DataSet(Series('id', 'key1'), start, end, [DataPoint(start, 12.34)], Summary())]
        requests_get.assert_called_once_with(
            'https://example.com:443/v1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00&key=key1',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(datasets, expected)

    @mock.patch('requests.post')
    def test_write_id(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 12.34)]
        result = self.client.write_id("id1", data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/series/id/id1/data/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 12.34}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    @mock.patch('requests.post')
    def test_write_key(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 12.34)]
        result = self.client.write_key("key1", data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/series/key/key1/data/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 12.34}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    @mock.patch('requests.post')
    def test_increment_id(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 1)]
        result = self.client.increment_id("id1", data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/series/id/id1/increment/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 1}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    @mock.patch('requests.post')
    def test_increment_key(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 1)]
        result = self.client.increment_key("key1", data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/series/key/key1/increment/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 1}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    @mock.patch('requests.post')
    def test_write_bulk(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [
            { 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4.164 },
            { 'id': '38268c3b231f1266a392931e15e99231', 'v': 73.13 },
            { 'key': 'your-custom-key', 'v': 55.423 },
            { 'key': 'foo', 'v': 324.991 },
        ]
        ts = datetime.datetime(2012, 3, 27)
        result = self.client.write_bulk(ts, data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/data/',
            auth=('key', 'secret'),
            data="""{"data": %s, "t": "2012-03-27T00:00:00"}""" % simplejson.dumps(data),
            headers=self.post_headers
        )
        self.assertEqual(result, '')

    @mock.patch('requests.post')
    def test_increment_bulk(self, requests_post):
        requests_post.return_value = MockResponse(200, "")
        data = [
            { 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4 },
            { 'id': '38268c3b231f1266a392931e15e99231', 'v': 2 },
            { 'key': 'your-custom-key', 'v': 1 },
            { 'key': 'foo', 'v': 1 },
        ]
        ts = datetime.datetime(2012, 3, 27)
        result = self.client.increment_bulk(ts, data)

        requests_post.assert_called_once_with(
            'https://example.com:443/v1/increment/',
            auth=('key', 'secret'),
            data="""{"data": %s, "t": "2012-03-27T00:00:00"}""" % simplejson.dumps(data),
            headers=self.post_headers
        )
        self.assertEqual(result, '')
