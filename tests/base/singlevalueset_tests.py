#!/usr/bin/env python
# encoding: utf-8

import datetime
from unittest2 import TestCase

from tempodb import DataPoint, SingleValueSet, Series


class SingleValueSetTest(TestCase):

    def test_init(self):
        ts = datetime.datetime(2012, 1, 1)
        series = Series("id", "key")
        data = DataPoint(ts, 12.34)
        svset = SingleValueSet(series, data)
        self.assertEqual(svset.series, series)
        self.assertEqual(svset.data, data)

    def test_from_json(self):
        json = {
            'series': {
                'id': 'id',
                'key': 'key',
                'name': 'name',
                'tags': ['tag1', 'tag2'],
                'attributes': {'key1': 'value1'},
            },
            'data': { 't': '2013-11-07T00:00:00', 'v': 45.5 }
        }
        svset = SingleValueSet.from_json(json)
        series = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        ts = datetime.datetime(2013, 11, 7, 0, 0, 0, 0)
        expected = SingleValueSet(series, DataPoint(ts, 45.5))
        self.assertEqual(svset, expected)

    def test_from_json_with_null(self):
        json = {
            'series': {
                'id': 'id',
                'key': 'key',
                'name': 'name',
                'tags': ['tag1', 'tag2'],
                'attributes': {'key1': 'value1'},
            },
            'data': None
        }
        svset = SingleValueSet.from_json(json)
        series = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        expected = SingleValueSet(series, None)
        self.assertEqual(svset, expected)
