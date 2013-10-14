#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/client.py

Copyright (c) 2012 TempoDB, Inc. All rights reserved.
"""

import datetime
import re
import requests
import simplejson
import urllib
import urllib2

import tempodb
from tempodb import DataPoint, DataSet, DeleteSummary, Series, Summary


API_HOST = 'api.tempo-db.com'
API_PORT = 443
API_VERSION = 'v1'

VALID_SERIES_KEY = r'^[a-zA-Z0-9\.:;\-_/\\ ]*$'
RE_VALID_SERIES_KEY = re.compile(VALID_SERIES_KEY)

DATETIME_HANDLER = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None


class Client(object):

    def __init__(self, key, secret, host=API_HOST, port=API_PORT, secure=True, pool_connections=10, pool_maxsize=10):
        self.key = key
        self.secret = secret
        self.host = host
        self.port = port
        self.secure = secure
        self.session = requests.session()
        self.session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize))
        self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize))

    def get_series(self, ids=[], keys=[], tags=[], attributes={}):
        params = self._normalize_params(ids, keys, tags, attributes)
        json = self.request('/series/', method='GET', params=params)
        series = [Series.from_json(s) for s in json]
        return series

    def delete_series(self, ids=[], keys=[], tags=[], attributes={}, allow_truncation=False):
        params = self._normalize_params(ids, keys, tags, attributes)
        params['allow_truncation'] = allow_truncation
        json = self.request('/series/', method='DELETE', params=params)
        return DeleteSummary.from_json(json)

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

    def delete_id(self, series_id, start, end, **kwargs):
        series_type = 'id'
        series_val = series_id
        return self._delete(series_type, series_val, start, end, kwargs)

    def delete_key(self, series_key, start, end, **kwargs):
        series_type = 'key'
        series_val = series_key
        return self._delete(series_type, series_val, start, end, kwargs)

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

    def write_multi(self, data):
        json = self.request('/multi/', method='POST', params=data)
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

    def increment_multi(self, data):
        json = self.request('/multi/increment/', method='POST', params=data)
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

        url = '/series/%s/%s/data/' % (series_type, urllib2.quote(series_val, ""))
        json = self.request(url, method='GET', params=params)

        #we got an error
        if 'error' in json:
            return json
        return DataSet.from_json(json)

    def _delete(self, series_type, series_val, start, end, options):
        params = {
            'start': start.isoformat(),
            'end': end.isoformat(),
        }
        params.update(options)
        url = '/series/%s/%s/data/' % (series_type, urllib2.quote(series_val, ""))
        json = self.request(url, method='DELETE', params=params)
        return json

    def _write(self, series_type, series_val, data):
        url = '/series/%s/%s/data/' % (series_type, urllib2.quote(series_val, ""))
        body = [dp.to_json() for dp in data]
        json = self.request(url, method='POST', params=body)
        return json

    def _increment(self, series_type, series_val, data):
        url = '/series/%s/%s/increment/' % (series_type, urllib2.quote(series_val, ""))
        body = [dp.to_json() for dp in data]
        json = self.request(url, method='POST', params=body)
        return json

    def request(self, target, method='GET', params={}):
        assert method in ['GET', 'POST', 'PUT', 'DELETE'], "Only 'GET', 'POST', 'PUT', 'DELETE' are allowed for method."

        headers = {
            'User-Agent': 'tempodb-python/%s' % (tempodb.get_version(), ),
            'Accept-Encoding': 'gzip',
        }

        if method == 'POST':
            headers['Content-Type'] = "application/json"
            base = self.build_full_url(target)
            json_data = simplejson.dumps(params, default=DATETIME_HANDLER)
            response = self.session.post(base, data=json_data, auth=(self.key, self.secret), headers=headers)
        elif method == 'PUT':
            headers['Content-Type'] = "application/json"
            base = self.build_full_url(target)
            json_data = simplejson.dumps(params, default=DATETIME_HANDLER)
            response = self.session.put(base, data=json_data, auth=(self.key, self.secret), headers=headers)
        elif method == 'DELETE':
            base = self.build_full_url(target, params)
            response = self.session.delete(base, auth=(self.key, self.secret), headers=headers)
        else:
            base = self.build_full_url(target, params)
            response = self.session.get(base, auth=(self.key, self.secret), headers=headers)

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
        default_port = {80: not self.secure, 443: self.secure}
        port = "" if default_port.get(self.port, False) else ":%d" % self.port
        protocol = "https://" if self.secure else "http://"
        base_full_url = "%s%s%s" % (protocol, self.host, port)
        return base_full_url + self.build_url(target, params)

    def build_url(self, url, params={}):
        if params:
            return "/%s%s?%s" % (API_VERSION, url, self._urlencode(params))
        else:
            return "/%s%s" % (API_VERSION, url)

    def _urlencode(self, params):
        p = []
        for key, value in params.iteritems():
            if isinstance(value, (list, tuple)):
                for v in value:
                    p.append((key, v))
            elif isinstance(value, dict):
                for k, v in value.items():
                    p.append(('%s[%s]' % (key, k), v))
            elif isinstance(value, bool):
                p.append((key, str(value).lower()))
            else:
                p.append((key, str(value)))
        return urllib.urlencode(p).encode("UTF-8")

    def _normalize_params(self, ids=[], keys=[], tags=[], attributes={}):
        params = {}
        if ids:
            params['id'] = ids
        if keys:
            params['key'] = keys
        if tags:
            params['tag'] = tags
        if attributes:
            params['attr'] = attributes
        return params


class TempoDBApiException(Exception):
    pass
