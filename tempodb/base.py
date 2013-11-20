#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/client.py

Copyright (c) 2012 TempoDB, Inc. All rights reserved.
"""

import simplejson
from dateutil import parser


class Database(object):

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret


class Series(object):

    def __init__(self, i, key, name="", attributes={}, tags=[]):
        self.id = i
        self.key = key
        self.name = name
        self.attributes = attributes
        self.tags = tags

    def to_json(self):
        return self.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
       return self.__dict__ == other.__dict__

    @staticmethod
    def from_json(json):
        i = json.get('id', '')
        key = json.get('key', '')
        attributes = json.get('attributes', {})
        tags = json.get('tags', [])
        name = json.get('name', '')
        series = Series(i, key, name=name, attributes=attributes, tags=tags)
        return series


class DataPoint(object):

    def __init__(self, ts, value):
        self.ts = ts
        self.value = value

    def __str__(self):
        return "t: %s, v: %s" % (self.ts, self.value)

    def __eq__(self, other):
       return self.__dict__ == other.__dict__

    def to_json(self):
        json = {
            't': self.ts.isoformat(),
            'v': self.value,
        }
        return json

    @staticmethod
    def from_json(json):
        ts = parser.parse(json.get('t', ''))
        value = json.get('v', None)
        dp = DataPoint(ts, value)
        return dp


class DataSet(object):

    def __init__(self, series, start, end, data=[], summary=None):
        self.series = series
        self.start = start
        self.end = end
        self.data = data
        self.summary = summary

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
       return self.__dict__ == other.__dict__

    @staticmethod
    def from_json(json):
        series = Series.from_json(json.get('series', {}))

        start_date = parser.parse(json.get('start', ''))
        end_date = parser.parse(json.get('end', ''))

        data = [DataPoint.from_json(dp) for dp in json.get("data", [])]
        summary = Summary.from_json(json.get('summary', {})) if 'summary' in json else None
        return DataSet(series, start_date, end_date, data, summary)


class Summary(object):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @staticmethod
    def from_json(json):
        summary = Summary()
        summary.__dict__.update(json)
        return summary

class DeleteSummary(object):
    def __init__(self, deleted):
        self.deleted = deleted

    @staticmethod
    def from_json(json):
        return DeleteSummary(json['deleted'])


class SingleValueSet(object):
    def __init__(self, series, data):
        self.series = series
        self.data = data

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @staticmethod
    def from_json(json):
        series = Series.from_json(json.get('series', {}))
        data_or_null = json.get('data', None)
        data = DataPoint.from_json(data_or_null) if data_or_null != None else None
        return SingleValueSet(series, data)
