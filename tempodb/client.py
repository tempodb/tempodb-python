#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/client.py

Copyright (c) 2012 TempoDB, Inc. All rights reserved.
"""

import re
import requests
import simplejson
import urllib
import urllib2

import tempodb
from tempodb import DataPoint, DataSet, Series, Summary


API_HOST = 'api.tempo-db.com'
API_PORT = 443
API_VERSION = 'v1'

VALID_SERIES_KEY = r'^[a-zA-Z0-9\.:;\-_]+$'
RE_VALID_SERIES_KEY = re.compile(VALID_SERIES_KEY)


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

    def create_series(self, key=None):
        if key and not RE_VALID_SERIES_KEY.match(key):
            raise ValueError("Series key must match the following regex: %s" % (VALID_SERIES_KEY,))

        params = {}
        if key is not None:
            params['key'] = key

        json = self.request('/series/', method='POST', params=params)
        series = Series.from_json(json)
        return series

    def update_series(self, series):
        json = self.request('/series/id/%s/' % (series.id,), method='PUT', params=series.to_json())
        series = Series.from_json(json)
        return series

    def read(self, start, end, interval="", function="", ids=[], keys=[], tags=[], attributes={}, tz=""):
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
        if tz:
            params['tz'] = tz

        url = '/data/'
        json = self.request(url, method='GET', params=params)
        return [DataSet.from_json(j) for j in json]

    def read_id(self, series_id, start, end, interval="", function="", tz=""):
        series_type = 'id'
        series_val = series_id
        return self._read(series_type, series_val, start, end, interval, function, tz)

    def read_key(self, series_key, start, end, interval="", function="", tz=""):
        series_type = 'key'
        series_val = series_key
        return self._read(series_type, series_val, start, end, interval, function, tz)

    def delete_id(self, series_id, start, end):
        series_type = 'id'
        series_val = series_id
        return self._delete(series_type, series_val, start, end)

    def delete_key(self, series_key, start, end):
        series_type = 'key'
        series_val = series_key
        return self._delete(series_type, series_val, start, end)

    def write_id(self, series_id, data):
        series_type = 'id'
        series_val = series_id
        return self._write(series_type, series_val, data)

    def write_key(self, series_key, data):
        if series_key and not RE_VALID_SERIES_KEY.match(series_key):
            raise ValueError("Series key must match the following regex: %s" % (VALID_SERIES_KEY,))

        series_type = 'key'
        series_val = series_key
        return self._write(series_type, series_val, data)

    def write_bulk(self, ts, data):
        body = {
            't': ts.isoformat(),
            'data': data
        }
        json = self.request('/data/', method='POST', params=body)
        return json

    def increment_id(self, series_id, data):
        series_type = 'id'
        series_val = series_id
        return self._increment(series_type, series_val, data)

    def increment_key(self, series_key, data):
        if series_key and not RE_VALID_SERIES_KEY.match(series_key):
            raise ValueError("Series key must match the following regex: %s" % (VALID_SERIES_KEY,))

        series_type = 'key'
        series_val = series_key
        return self._increment(series_type, series_val, data)

    def increment_bulk(self, ts, data):
        body = {
            't': ts.isoformat(),
            'data': data
        }
        json = self.request('/increment/', method='POST', params=body)
        return json

    def _read(self, series_type, series_val, start, end, interval="", function="", tz=""):
        params = {
            'start': start.isoformat(),
            'end': end.isoformat(),
        }

        # add rollup interval and function if supplied
        if interval:
            params['interval'] = interval
        if function:
            params['function'] = function
        if tz:
            params['tz'] = tz

        url = '/series/%s/%s/data/' % (series_type, series_val)
        json = self.request(url, method='GET', params=params)

        #we got an error
        if 'error' in json:
            return json
        return DataSet.from_json(json)

    def _delete(self, series_type, series_val, start, end):
        params = {
            'start': start.isoformat(),
            'end': end.isoformat(),
        }
        url = '/series/%s/%s/data/' % (series_type, series_val)
        json = self.request(url, method='DELETE', params=params)
        return json

    def _write(self, series_type, series_val, data):
        url = '/series/%s/%s/data/' % (series_type, series_val)
        body = [dp.to_json() for dp in data]
        json = self.request(url, method='POST', params=body)
        return json

    def _increment(self, series_type, series_val, data):
        url = '/series/%s/%s/increment/' % (series_type, series_val)
        body = [dp.to_json() for dp in data]
        json = self.request(url, method='POST', params=body)
        return json

    def request(self, target, method='GET', params={}):
        assert method in ['GET', 'POST', 'PUT', 'DELETE'], "Only 'GET', 'POST', 'PUT', 'DELETE' are allowed for method."

        headers = {
            'User-Agent': 'tempodb-python/%s' % (tempodb.get_version(), )
        }

        if method == 'POST':
            headers['Content-Type'] = "application/json"
            base = self.build_full_url(target)
            response = requests.post(base, data=simplejson.dumps(params), auth=(self.key, self.secret), headers=headers)
        elif method == 'PUT':
            headers['Content-Type'] = "application/json"
            base = self.build_full_url(target)
            response = requests.put(base, data=simplejson.dumps(params), auth=(self.key, self.secret), headers=headers)
        elif method == 'DELETE':
            base = self.build_full_url(target, params)
            response = requests.delete(base, auth=(self.key, self.secret), headers=headers)
        else:
            base = self.build_full_url(target, params)
            response = requests.get(base, auth=(self.key, self.secret), headers=headers)

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
