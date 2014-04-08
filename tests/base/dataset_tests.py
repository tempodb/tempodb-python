#!/usr/bin/env python
# encoding: utf-8

import datetime
from unittest2 import TestCase

from tempodb import DataPoint, DataSet, Series, Summary


class DataSetTest(TestCase):

    def test_init(self):
        start = datetime.datetime(2012, 1, 1)
        end = datetime.datetime(2012, 1, 2)
        series = Series("id", "key")
        data = [DataPoint(start, 12.34), DataPoint(end, 23.45)]
        summary = Summary()
        dataset = DataSet(series, start, end, data, summary)
        self.assertEqual(dataset.series, series)
        self.assertEqual(dataset.start, start)
        self.assertEqual(dataset.end, end)
        self.assertEqual(dataset.data, data)
        self.assertEqual(dataset.summary, summary)

    def test_from_json(self):
        json = {
            'series': {
                'id': 'id',
                'key': 'key',
                'name': 'name',
                'tags': ['tag1', 'tag2'],
                'attributes': {'key1': 'value1'},
            },
            'start': '2012-03-27T00:00:00.000Z',
            'end': '2012-03-28T00:00:00.000Z',
            'data': [],
            'summary': {'min': 45.5}
        }
        dataset = DataSet.from_json(json)
        series = Series('id', 'key', 'name', {'key1': 'value1'}, ['tag1', 'tag2'])
        start = datetime.datetime(2012, 3, 27)
        end = datetime.datetime(2012, 3, 28)
        data = []
        summary = Summary(min=45.5)
        expected = DataSet(series, start, end, data, summary)
        self.assertEqual(dataset, expected)
