#!/usr/bin/env python
# encoding: utf-8

from unittest2 import TestCase

from tempodb import Series


class SeriesTest(TestCase):

    def test_init(self):
        series = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        self.assertEqual(series.id, 'id')
        self.assertEqual(series.key, 'key')
        self.assertEqual(series.name, 'name')
        self.assertEqual(series.attributes, {'key1': 'value1'})
        self.assertEqual(series.tags, ['tag1', 'tag2'])

    def test_to_json(self):
        series = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        json = series.to_json()
        expected = {
            'id': 'id',
            'key': 'key',
            'name': 'name',
            'tags': ['tag1', 'tag2'],
            'attributes': {'key1': 'value1'},
        }
        self.assertEqual(json, expected)

    def test_from_json(self):
        json = {
            "id": "id",
            "key": "key",
            "name": "name",
            "tags": ["tag1", "tag2"],
            "attributes": {"key1": "value1"}
        }
        series = Series.from_json(json)
        expected = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        self.assertEqual(series, expected)
