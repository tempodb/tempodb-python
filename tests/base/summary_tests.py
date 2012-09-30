#!/usr/bin/env python
# encoding: utf-8

from unittest2 import TestCase

from tempodb import Summary


class SummaryTest(TestCase):

    def test_init(self):
        summary = Summary(min=45.6, max=23.4)
        self.assertEqual(summary.min, 45.6)
        self.assertEqual(summary.max, 23.4)

    def test_from_json(self):
        json = {
            'sum': 12,
            'stddev': 23.4
        }
        summary = Summary.from_json(json)
        expected = Summary(sum=12, stddev=23.4)
        self.assertEqual(summary, expected)
