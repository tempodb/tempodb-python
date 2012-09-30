#!/usr/bin/env python
# encoding: utf-8

import mock
import requests
from unittest2 import TestCase

import tempodb
from tempodb import Client, Series


class MockRequest(object):

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
        requests_get.return_value = MockRequest(200, """[{
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
