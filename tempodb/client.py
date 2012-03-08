
#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/client.py

Copyright (c) 2012 TempoDB, Inc. All rights reserved.
"""

from dateutil import parser
import requests
import simplejson
import urllib
import urllib2


API_HOST = 'api.tempo-db.com'
API_PORT = 443
API_VERSION = 'v1'


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

    @staticmethod
    def from_json(json):
        series = Series.from_json(json.get('series', {}))

        start_date = parser.parse(json.get('start', ''))
        end_date = parser.parse(json.get('end', ''))

        data = [DataPoint.from_json(dp) for dp in json.get("data", [])]
        summary = Summary.from_json(json.get('summary', {})) if 'summary' in json else None
        return DataSet(series, start_date, end_date, data, summary)


class Summary(object):

    @staticmethod
    def from_json(json):
        summary = Summary()
        summary.__dict__.update(json)
        return summary


class Client(object):

    def __init__(self, key, secret, host=API_HOST, port=API_PORT, secure=True):
        self.key = key
        self.secret = secret
        self.host = host
        self.port = port
        self.secure = secure

    def create_database(self, name=""):
        params = {
            'name': name,
        }
        json = self.request('/database/', method='POST', params=params)
        key = json.get('id', '')
        secret = json.get('password', '')
        database = Database(key, secret)
        return database

    def get_series(self, ids=[], keys=[], tags=[], attributes={}):
        params = {}
        if ids:
            params['id'] = ids
        if keys:
            params['key'] = keys
        if tags:
            params['tag'] = tags
        if attributes:
            params['attr'] = attributes

        json = self.request('/series/', method='GET', params=params)
        series = [Series.from_json(s) for s in json]
        return series

    def update_series(self, series):
        json = self.request('/series/id/%s/' % (series.id,), method='PUT', params=series.to_json())
        series = Series.from_json(json)
        return series

    def read(self, start, end, interval="", function="", ids=[], keys=[], tags=[], attributes={}):
        params = {
            'start': start.isoformat(),
            'end': end.isoformat()
        }

        if ids:
            params['id'] = ids
        if keys:
            params['key'] = keys
        if interval:
            params['interval'] = interval
        if function:
            params['function'] = function
        if tags:
            params['tag'] = tags
        if attributes:
            params['attr'] = attributes

        url = '/data/'
        json = self.request(url, method='GET', params=params)
        return [DataSet.from_json(j) for j in json]

    def read_id(self, series_id, start, end, interval="", function=""):
        series_type = 'id'
        series_val = series_id
        return self._read(series_type, series_val, start, end, interval, function)

    def read_key(self, series_key, start, end, interval="", function=""):
        series_type = 'key'
        series_val = series_key
        return self._read(series_type, series_val, start, end, interval, function)

    def _read(self, series_type, series_val, start, end, interval="", function=""):
        params = {
            'start': start.isoformat(),
            'end': end.isoformat(),
        }

        # add rollup interval and function if supplied
        if interval:
            params['interval'] = interval
        if function:
            params['function'] = function

        url = '/series/%s/%s/data/' % (series_type, series_val)
        json = self.request(url, method='GET', params=params)

        #we got an error
        if 'error' in json:
            return json
        return DataSet.from_json(json)

    def write_id(self, series_id, data):
        series_type = 'id'
        series_val = series_id
        return self.write(series_type, series_val, data)

    def write_key(self, series_key, data):
        series_type = 'key'
        series_val = series_key
        return self.write(series_type, series_val, data)

    def write(self, series_type, series_val, data):
        url = '/series/%s/%s/data/' % (series_type, series_val)
        body = [dp.to_json() for dp in data]
        json = self.request(url, method='POST', params=body)
        return json

    def write_bulk(self, ts, data):
        body = {
            't': ts.isoformat(),
            'data': data
        }
        json = self.request('/data/', method='POST', params=body)
        return json

    def request(self, target, method='GET', params={}):
        assert method in ['GET', 'POST', 'PUT'], "Only 'GET' and 'POST' are allowed for method."

        if method == 'POST':
            base = self.build_full_url(target)
            response = requests.post(base, data=simplejson.dumps(params), auth=(self.key, self.secret))
        elif method == 'PUT':
            base = self.build_full_url(target)
            response = requests.put(base, data=simplejson.dumps(params), auth=(self.key, self.secret))
        else:
            base = self.build_full_url(target, params)
            response = requests.get(base, auth=(self.key, self.secret))

        if response.status_code == 200:
            if response.text:
                json = simplejson.loads(response.text)
            else:
                json = ''
            #try:
            #    json = simplejson.loads(response.text)
            #except simplejson.decoder.JSONDecodeError, err:
            #    json = dict(error="JSON Parse Error (%s):\n%s" % (err, response.text))
        else:
            json = dict(error=response.text)
        return json

    def build_full_url(self, target, params={}):
        port = "" if self.port == 80 else ":%d" % self.port
        protocol = "https://" if self.secure else "http://"
        base_full_url = "%s%s%s" % (protocol, self.host, port)
        return base_full_url + self.build_url(target, params)

    def build_url(self, url, params={}):
        target_path = urllib2.quote(url)

        if params:
            return "/%s%s?%s" % (API_VERSION, target_path, self._urlencode(params))
        else:
            return "/%s%s" % (API_VERSION, target_path)

    def _urlencode(self, params):
        p = []
        for key, value in params.iteritems():
            if isinstance(value, (list, tuple)):
                for v in value:
                    p.append((key, v))
            elif isinstance(value, dict):
                for k, v in value.items():
                    p.append(('%s[%s]' % (key, k), v))
            else:
                p.append((key, value))
        return urllib.urlencode(p).encode("UTF-8")


class TempoDBApiException(Exception):
    pass
