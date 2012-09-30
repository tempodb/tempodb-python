#!/usr/bin/env python
# encoding: utf-8

from unittest2 import TestCase

from tempodb import Database


class DatabaseTest(TestCase):

    def test_init(self):
        database = Database('key', 'secret')
        self.assertEqual(database.key, 'key')
        self.assertEqual(database.secret, 'secret')
