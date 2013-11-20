#!/usr/bin/env python
# encoding: utf-8

import datetime
import mock
import requests
import simplejson
from unittest2 import TestCase

import tempodb
from tempodb import Client, DataPoint, DataSet, Series, SingleValueSet, Summary


class MockResponse(object):

    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


class ClientTest(TestCase):

    def setUp(self):
        self.client = Client('key', 'secret', 'example.com', 443, True)
        self.client.session = mock.Mock()
        self.headers = {
            'User-Agent': 'tempodb-python/%s' % tempodb.get_version(),
            'Accept-Encoding': 'gzip',
        }
        self.get_headers = self.headers
        self.delete_headers = self.headers
        self.put_headers = dict({
            'Content-Type': 'application/json',
        }, **self.headers)
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

    def test_port_defaults(self):
        """ 80 is the default port for HTTP, 443 is the default for HTTPS """
        client = Client('key', 'secret', 'example.com', 80, False)
        self.assertEqual(client.build_full_url('/etc'), 'http://example.com/v1/etc')
        client = Client('key', 'secret', 'example.com', 88, False)
        self.assertEqual(client.build_full_url('/etc'), 'http://example.com:88/v1/etc')
        client = Client('key', 'secret', 'example.com', 443, False)
        self.assertEqual(client.build_full_url('/etc'), 'http://example.com:443/v1/etc')
        client = Client('key', 'secret', 'example.com', 443, True)
        self.assertEqual(client.build_full_url('/etc'), 'https://example.com/v1/etc')
        client = Client('key', 'secret', 'example.com', 88, True)
        self.assertEqual(client.build_full_url('/etc'), 'https://example.com:88/v1/etc')
        client = Client('key', 'secret', 'example.com', 80, True)
        self.assertEqual(client.build_full_url('/etc'), 'https://example.com:80/v1/etc')

    def test_get_series(self):
        self.client.session.get.return_value = MockResponse(200, """[{
            "id": "id",
            "key": "key",
            "name": "name",
            "tags": ["tag1", "tag2"],
            "attributes": {"key1": "value1"}
        }]""")

        series = self.client.get_series()
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        expected = [Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])]
        self.assertEqual(series, expected)

    def test_delete_series(self):
        self.client.session.delete.return_value = MockResponse(200, """{"deleted":2}""")

        summary = self.client.delete_series([], [], [], {'key': 'one', 'key2': 'two'})
        self.assertEqual(summary.deleted, 2)

    def test_create_series(self):
        self.client.session.post.return_value = MockResponse(200, """{
            "id": "id",
            "key": "my-key.tag1.1",
            "name": "",
            "tags": ["my-key", "tag1"],
            "attributes": {}
        }""")
        series = self.client.create_series("my-key.tag1.1")

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/',
            data="""{"key": "my-key.tag1.1"}""",
            auth=('key', 'secret'),
            headers=self.post_headers
        )
        expected = Series('id', 'my-key.tag1.1', '', {}, ['my-key', 'tag1'])
        self.assertEqual(series, expected)

    def test_create_series_validity_error(self):
        with self.assertRaises(ValueError):
            series = self.client.create_series('key.b%^.test')

    def test_update_series(self):
        update = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1'])
        self.client.session.put.return_value = MockResponse(200, simplejson.dumps(update.to_json()))

        updated = self.client.update_series(update)

        self.client.session.put.assert_called_once_with(
            'https://example.com/v1/series/id/id/',
            auth=('key', 'secret'),
            data=simplejson.dumps(update.to_json()),
            headers=self.put_headers
        )
        self.assertEqual(update, updated)

    def test_read_id(self):
        self.client.session.get.return_value = MockResponse(200, """{
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
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/id/id/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    def test_read_key(self):
        self.client.session.get.return_value = MockResponse(200, """{
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
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/key/key1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    def test_read_key_escape(self):
        self.client.session.get.return_value = MockResponse(200, """{
            "series": {
                "id": "id",
                "key": "ke:y/1",
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
        dataset = self.client.read_key('ke:y/1', start, end)

        expected = DataSet(Series('id', 'ke:y/1'), start, end, [DataPoint(start, 12.34)], Summary())
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/key/ke%3Ay%2F1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    def test_read(self):
        self.client.session.get.return_value = MockResponse(200, """[{
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
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00&key=key1',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(datasets, expected)

    def test_delete_id(self):
        self.client.session.delete.return_value = MockResponse(200, "")
        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        result = self.client.delete_id("id1", start, end)

        self.client.session.delete.assert_called_once_with(
            'https://example.com/v1/series/id/id1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.delete_headers
        )
        self.assertEquals(result, '')

    def test_delete_key(self):
        self.client.session.delete.return_value = MockResponse(200, "")
        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        result = self.client.delete_key("key1", start, end)

        self.client.session.delete.assert_called_once_with(
            'https://example.com/v1/series/key/key1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.delete_headers
        )
        self.assertEquals(result, '')

    def test_delete_key_escape(self):
        self.client.session.delete.return_value = MockResponse(200, "")
        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        result = self.client.delete_key("ke:y/1", start, end)

        self.client.session.delete.assert_called_once_with(
            'https://example.com/v1/series/key/ke%3Ay%2F1/data/?start=2012-03-27T00%3A00%3A00&end=2012-03-28T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.delete_headers
        )
        self.assertEquals(result, '')

    def test_write_id(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 12.34)]
        result = self.client.write_id("id1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/id/id1/data/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 12.34}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_write_key(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 12.34)]
        result = self.client.write_key("key1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/key/key1/data/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 12.34}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_write_key_escape(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 12.34)]
        result = self.client.write_key("ke:y/1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/key/ke%3Ay%2F1/data/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 12.34}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_increment_id(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 1)]
        result = self.client.increment_id("id1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/id/id1/increment/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 1}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_increment_key(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 1)]
        result = self.client.increment_key("key1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/key/key1/increment/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 1}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_increment_key_escape(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [DataPoint(datetime.datetime(2012, 3, 27), 1)]
        result = self.client.increment_key("ke:y/1", data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/series/key/ke%3Ay%2F1/increment/',
            auth=('key', 'secret'),
            data="""[{"t": "2012-03-27T00:00:00", "v": 1}]""",
            headers=self.post_headers
        )
        self.assertEquals(result, '')

    def test_write_bulk(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [
            { 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4.164 },
            { 'id': '38268c3b231f1266a392931e15e99231', 'v': 73.13 },
            { 'key': 'your-custom-key', 'v': 55.423 },
            { 'key': 'foo', 'v': 324.991 },
        ]
        ts = datetime.datetime(2012, 3, 27)
        result = self.client.write_bulk(ts, data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/data/',
            auth=('key', 'secret'),
            data="""{"data": %s, "t": "2012-03-27T00:00:00"}""" % simplejson.dumps(data),
            headers=self.post_headers
        )
        self.assertEqual(result, '')

    def test_increment_bulk(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [
            { 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4 },
            { 'id': '38268c3b231f1266a392931e15e99231', 'v': 2 },
            { 'key': 'your-custom-key', 'v': 1 },
            { 'key': 'foo', 'v': 1 },
        ]
        ts = datetime.datetime(2012, 3, 27)
        result = self.client.increment_bulk(ts, data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/increment/',
            auth=('key', 'secret'),
            data="""{"data": %s, "t": "2012-03-27T00:00:00"}""" % simplejson.dumps(data),
            headers=self.post_headers
        )
        self.assertEqual(result, '')


    def test_write_multi(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [
            { 't': datetime.datetime(2013, 8, 21), 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4.164 },
            { 't': datetime.datetime(2013, 8, 22), 'id': '38268c3b231f1266a392931e15e99231', 'v': 73.13 },
            { 't': datetime.datetime(2013, 8, 23), 'key': 'your-custom-key', 'v': 55.423 },
            { 't': datetime.datetime(2013, 8, 24), 'key': 'foo', 'v': 324.991 },
        ]
        result = self.client.write_multi(data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/multi/',
            auth=('key', 'secret'),
            data= simplejson.dumps(data, default=tempodb.DATETIME_HANDLER),
            headers=self.post_headers
        )
        self.assertEqual(result, '')

    def test_write_multi_207(self):
        expected_response = """{
            "{'error': {"multistatus": [
                { "status": "422", "messages": [ "Must provide a series ID or key" ] },
                { "status": "200", "messages": [] },
                { "status": "422", "messages": [ "Must provide a numeric value", "Must provide a series ID or key" ] }
            ]}}"""
        self.client.session.post.return_value = MockResponse(207, expected_response)

        data = [
            { 't': datetime.datetime(2013, 8, 21), 'v': 4.164 },
            { 't': datetime.datetime(2013, 8, 22), 'id': '38268c3b231f1266a392931e15e99231'},
            {}
        ]
        result = self.client.write_multi(data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/multi/',
            auth=('key', 'secret'),
            data= simplejson.dumps(data, default=tempodb.DATETIME_HANDLER),
            headers=self.post_headers
        )

        self.assertEqual(result["error"], expected_response)

    def test_increment_multi(self):
        self.client.session.post.return_value = MockResponse(200, "")
        data = [
            { 't': datetime.datetime(2013, 8, 21), 'id': '01868c1a2aaf416ea6cd8edd65e7a4b8', 'v': 4164 },
            { 't': datetime.datetime(2013, 8, 22), 'id': '38268c3b231f1266a392931e15e99231', 'v': 7313 },
            { 't': datetime.datetime(2013, 8, 23), 'key': 'your-custom-key', 'v': 55423 },
            { 't': datetime.datetime(2013, 8, 24), 'key': 'foo', 'v': 324991 },
        ]
        result = self.client.write_multi(data)

        self.client.session.post.assert_called_once_with(
            'https://example.com/v1/multi/',
            auth=('key', 'secret'),
            data= simplejson.dumps(data, default=tempodb.DATETIME_HANDLER),
            headers=self.post_headers
        )
        self.assertEqual(result, '')

    def test_single_value_id(self):
        self.client.session.get.return_value = MockResponse(200, """{
            "series": {
                "id": "id",
                "key": "key1",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {"t": "2012-03-27T00:00:00.000", "v": 12.34}
        }""")

        ts = datetime.datetime(2012, 3, 27)
        dataset = self.client.single_value_id('id', ts)

        expected = SingleValueSet(Series('id', 'key1'), DataPoint(ts, 12.34))
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/id/id/single/?ts=2012-03-27T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    def test_single_value_key(self):
        self.client.session.get.return_value = MockResponse(200, """{
            "series": {
                "id": "id",
                "key": "key1",
                "name": "",
                "tags": [],
                "attributes": {}
            },
            "data": {"t": "2012-03-27T00:00:00.000", "v": 12.34}
        }""")

        ts = datetime.datetime(2012, 3, 27)
        dataset = self.client.single_value_key('key1', ts)

        expected = SingleValueSet(Series('id', 'key1'), DataPoint(ts, 12.34))
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/series/key/key1/single/?ts=2012-03-27T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)

    def test_single_value(self):
        self.client.session.get.return_value = MockResponse(200, """[
            {
                "series": {
                    "id": "id",
                    "key": "key1",
                    "name": "",
                    "tags": ["tag"],
                    "attributes": {}
                },
                "data": {"t": "2012-03-27T01:00:00.000", "v": 34.12}
            },
            {
                "series": {
                    "id": "id2",
                    "key": "key2",
                    "name": "",
                    "tags": ["tag"],
                    "attributes": {}
                },
                "data": {"t": "2012-03-27T00:00:00.000", "v": 12.34}
            }
        ]""")

        ts1 = datetime.datetime(2012, 3, 27)
        ts2 = datetime.datetime(2012, 3, 27, 1, 0, 0, 0)
        dataset = self.client.single_value(ts1, direction='nearest', tags=['tag'])

        expected = [SingleValueSet(Series('id', 'key1', tags=['tag']), DataPoint(ts2, 34.12)),
                    SingleValueSet(Series('id2', 'key2', tags=['tag']), DataPoint(ts1, 12.34))]
        self.client.session.get.assert_called_once_with(
            'https://example.com/v1/single/?direction=nearest&tag=tag&ts=2012-03-27T00%3A00%3A00',
            auth=('key', 'secret'),
            headers=self.get_headers
        )
        self.assertEqual(dataset, expected)
