#!/usr/bin/env python
# encoding: utf-8

import datetime
from unittest import TestCase

from tempodb import DataPoint


class DataPointTest(TestCase):

    def test_init(self):
        now = datetime.datetime.now()
        dp = DataPoint(now, 12.34)
        self.assertEqual(dp.ts, now)
        self.assertEqual(dp.value, 12.34)

    def test_to_json(self):
        ts = datetime.datetime(2012, 3, 27, 1, 2, 3, 4)
        dp = DataPoint(ts, 12.34)
        expected = {
            't': '2012-03-27T01:02:03.000004',
            'v': 12.34
        }
        json = dp.to_json()
        self.assertEqual(json, expected)

    def test_from_json(self):
        json = {
            't': '2012-03-27T01:02:03.000004Z',
            'v': 12.34
        }
        dp = DataPoint.from_json(json)
        ts = datetime.datetime(2012, 3, 27, 1, 2, 3, 4)
        expected = DataPoint(ts, 12.34)
        self.assertEqual(dp, expected)
