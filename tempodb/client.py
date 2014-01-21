import functools
import urlparse
import urllib
import json
import endpoint
import protocol
from response import Response
from temporal.validate import check_time_param


def make_series_url(key):
    """Given a series key, generate a valid URL to the series endpoint for
    that key.

    :param string key: the series key
    :rtype: string"""

    url = urlparse.urljoin(endpoint.SERIES_ENDPOINT, 'key/')
    url = urlparse.urljoin(url, urllib.quote(key))
    return url


class with_response_type(object):
    """Decorator for ensuring the Response object returned by the
    :class:`Client` object has a data attribute that corresponds to the
    object type expected from the TempoDB API.  This class should not be
    used by user code.

    The "t" argument should be a string corresponding to the name of a class
    from the :mod:`tempodb.protocol.objects` module, or a single element list
    with the element being the name of a class from that module if the API
    endpoint will return a list of those objects.

    :param t: the type of object to cast the TempoDB response to
    :type t: list or string"""

    def __init__(self, t):
        self.t = t

    def __call__(self, f, *args, **kwargs):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            resp = f(*args, **kwargs)
            #dont try this at home kids
            session = args[0].session
            resp_obj = Response(resp, session)
            if resp_obj.status == 200:
                resp_obj._cast_payload(self.t)
            return resp_obj
        return wrapper


class Client(object):
    """Entry point class into the TempoDB API.  The client should be
    initialized with your API key and secret obtained from your TempoDB
    login.

    The methods are grouped as follows:

    SERIES

        * :meth:`create_series`
        * :meth:`delete_series`
        * :meth:`get_series`
        * :meth:`list_series`
        * :meth:`update_series`

    READING DATA

        * :meth:`read_data`
        * :meth:`find_data`
        * :meth:`aggregate_data`

    WRITING DATA

        * :meth:`write_data`
        * :meth:`write_multi`

    DELETING

        * :meth:`delete`

    SINGLE VALUE

        * :meth:`single_value`
        * :meth:`multi_series_single_value`

    :param string key: your API key
    :param string secret: your API secret"""

    def __init__(self, key, secret, base_url=endpoint.BASE_URL, pool_maxsize=10, pool_block=False):
        self.session = endpoint.HTTPEndpoint(key, secret, base_url, pool_maxsize, pool_block)

    #SERIES METHODS
    @with_response_type('Nothing')
    def create_series(self, key=None, tags=[], attrs={}):
        """Create a new series with an optional string key.  A list of tags
        and a map of attributes can also be optionally supplied.

        :param string key: (optional) a string key for the series
        :param list tags: (optional) the tags to create the series with
        :param dict attrs: (optional) the attributes to the create the series
                           with
        :rtype: :class:`tempodb.response.Response` object"""

        body = protocol.make_series_key(key, tags, attrs)
        resp = self.session.post(endpoint.SERIES_ENDPOINT, body)
        return resp

    @with_response_type('Nothing')
    def delete_series(self, key=None, tag=None, attr=None,
                      allow_truncation=False):
        """Delete a series according to the given criteria.

        **Note:** for the key argument, the filter will return the *union* of
        those values.  For the tag and attr arguments, the filter will return
        the *intersection* of those values.

        :param key: filter by one or more series keys
        :type key: list or string
        :param tag: filter by one or more tags
        :type tag: list or string
        :param dict attr: filter by one or more key-value attributes
        :param bool allow_truncation: whether to allow full deletion of a
                                      database
        :rtype: :class:`tempodb.response.Response` object"""

        params = {
            'key': key,
            'tag': tag,
            'attr': attr,
            'allow_truncation': str(allow_truncation).lower()
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([endpoint.SERIES_ENDPOINT, url_args])
        resp = self.session.delete(url)
        return resp

    @with_response_type('Series')
    def get_series(self, key):
        """Get a series object from TempoDB given its key.

        :param string key: a string name for the series
        :rtype: :class:`tempodb.response.Response` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'segment')
        resp = self.session.get(url)
        return resp

    def list_series(self, key=None, tag=None, attr=None,
                    limit=1000):
        """Get a list of all series matching the given criteria.

        **Note:** for the key argument, the filter will return the *union* of
        those values.  For the tag and attr arguments, the filter will return
        the *intersection* of those values.

        :param key: filter by one or more series keys
        :type key: list or string
        :param tag: filter by one or more tags
        :type tag: list or string
        :param dict attr: filter by one or more key-value attributes
        :rtype: :class:`tempodb.protocol.cursor.SeriesCursor` object"""

        params = {
            'key': key,
            'tag': tag,
            'attr': attr,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([endpoint.SERIES_ENDPOINT, url_args])
        resp = self.session.get(url)
        r = Response(resp, self.session)
        data = json.loads(r.resp.text)
        c = protocol.SeriesCursor(data, protocol.Series, r)
        return c

    @with_response_type('Series')
    def update_series(self, series):
        """Update a series with new attributes.  This does not change
        any of the data written to this series. The recommended workflow for
        series updates is to pull a Series object down using the
        :meth:`get_series` method, change its attributes, then pass it into
        this method.

        :param series: the series to update
        :type series: `tempodb.protocol.Series` object
        :rtype: :class:`tempodb.response.Response` object"""

        url = urlparse.urljoin(endpoint.SERIES_ENDPOINT, 'key/')
        url = urlparse.urljoin(url, series.key)

        resp = self.session.put(url, series.to_json())
        return resp

    #DATA READING METHODS
    def read_data(self, key, start=None, end=None, fold=None,
                  period=None, tz=None, limit=1000):
        """Read data from a series given its ID or key.  Start and end times
        must be supplied.  They can either be ISO8601 encoded strings (i.e.
        2012-01-08T00:21:54.000+0000) or Python Datetime objects, which will
        be converted for you.

        The function parameter is optional and can include string values such
        as "sum" and "avg".  This will apply a folding function to your
        rollup of data.  The optional interval parameter will downsample your
        data according to the given resolution ("1min", "2day", etc).

        Finally, the optional tz parameter can be used to specify a time zone
        for your output.  Please see
        `here <https://tempo-db.com/docs/api/timezone/>`_ for a list of a
        valid timezone values.

        :param string key: the series key to use
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string function: (optional) the name of a rollup function to use
        :param string interval: (optional) downsampling rate for the data
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'segment')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'rollup.fold': fold,
            'rollup.period': period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        r = Response(resp, self.session)
        data = json.loads(r.resp.text)
        c = protocol.DataPointCursor(data['data'], protocol.DataPoint, r,
                                     tz=data['tz'])
        return c

    def find_data(self, key, start, end, predicate, period, tz=None,
                  limit=1000):
        """Finds data from a given series according to a defined predicate
        function.  Start and end times must be supplied.  They can either be
        ISO8601 encoded strings (i.e. 2012-01-08T00:21:54.000+0000) or Python
        Datetime objects, which will be converted for you.

        The predicate and period must be supplied.  The period specifies
        sub-intervals from start to end in which the search will be performed
        (i.e. 1min will search over each minute within the interval).  The
        predicate can be one of "max", "min", "first", or "last" and will
        return the point over the given period that satisfies that predicate.

        Finally, the optional tz parameter can be used to specify a time zone
        for your output.  Please see
        `here <https://tempo-db.com/docs/api/timezone/>`_ for a list of a
        valid timezone values.

        :param string key: the series key to use
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string predicate: the name of a search function to use
        :param string interval: downsampling rate for the data
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointFindCursor` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'find')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'predicate.function': predicate,
            'rollup.period': period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        r = Response(resp, self.session)
        data = json.loads(r.resp.text)
        c = protocol.DataPointCursor(data['data'], protocol.DataPointFound, r,
                                     tz=data['tz'])
        return c

    def aggregate_data(self, aggregation, keys=[], tags=[], attrs={},
                       start=None, end=None, tz=None, rollupfold=None, period=None, limit=1000):
        """Read data from multiple series according to a filter and apply a
        function across all the returned series to put the datapoints together
        into one aggregrate series.

        See the :meth:`list_series` method for a description of how the filter
        criteria are applied, and the :meth:`read_data` method for how to
        work with the start, end, and tz parameters.

        Valid aggregation functions are the same as valid fold functions.

        :param key: (optional) filter by one or more series keys
        :type key: list or string
        :param tag: (optional) filter by one or more tags
        :type tag: list or string
        :param dict attr: (optional) filter by one or more key-value attributes
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` object"""

        url = 'segment'

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'key': keys,
            'tag': tags,
            'attr': attrs,
            'aggregation.fold': aggregation,
            'rollup.fold': rollupfold,
            'rollup.period': period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        r = Response(resp, self.session)
        data = json.loads(r.resp.text)
        c = protocol.DataPointCursor(data['data'], protocol.DataPoint, r,
                                     tz=data['tz'])
        return c

    #@with_response_type(['DataSet'])
    #def read_multi(self, key=None, start=None, end=None,
    #               function=None, interval=None, tz=None, tag=None,
    #               attr=None):
    #    """Read data from multiple series given filter criteria.  See the
    #    :meth:`list_series` method for a description of how the filter
    #    criteria are applied, and the :meth:`read_data` method for how to
    #    work with the start, end, function, interval, and tz parameters.
    #
    #    :param series_id: (optional) filter by one or more series IDs
    #    :type series_id: list or string
    #    :param key: (optional) filter by one or more series keys
    #    :type key: list or string
    #    :param tag: filter by one or more tags
    #    :type tag: list or string
    #    :param dict attr: (optional) filter by one or more key-value attributes
    #    :param start: the start time for the data points
    #    :type start: string or Datetime
    #    :param end: the end time for the data points
    #    :type end: string or Datetime
    #    :param string function: (optional) the name of a rollup function to use
    #    :param string interval: (optional) downsampling rate for the data
    #    :param string tz: (optional) the timezone to place the data into
    #    :rtype: :class:`tempodb.response.Response` object"""
    #
    #    url = 'data'
    #
    #    vstart = check_time_param(start)
    #    vend = check_time_param(end)
    #    params = {
    #        'key': key,
    #        'tag': tag,
    #        'attr': attr,
    #        'start': vstart,
    #        'end': vend,
    #        'function': function,
    #        'interval': interval,
    #        'tz': tz
    #    }
    #    url_args = endpoint.make_url_args(params)
    #    url = '?'.join([url, url_args])
    #    resp = self.session.get(url)
    #    return resp

    #WRITE DATA METHODS
    @with_response_type('Nothing')
    def write_data(self, key, data, tags=[], attrs={}):
        """Write a set a datapoints into a series by its key.  For now,
        the tags and attributes arguments are ignored.

        :param string key: the series to write data into
        :param list data: a list of DataPoints to write
        :rtype: :class:`tempodb.response.Response` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'data')

        #revisit later if there are server changes to take these into
        #account
        #params = {
        #    'tag': tag,
        #    'attr': attr,
        #}
        #url_args = endpoint.make_url_args(params)
        #url = '?'.join([url, url_args])

        dlist = [d.to_dictionary() for d in data]
        body = json.dumps(dlist)
        resp = self.session.post(url, body)
        return resp

    @with_response_type('Nothing')
    def write_multi(self, data):
        """Write a set a datapoints into multiple series by key or series ID.
        Each :class:`tempodb.protocol.objects.DataPoint` object should have
        either a key or id attribute set that indicates which series it will
        be written into::

            [
                {"t": "2012-...", "key": "foo", "v": 1},
                {"t": "2012-...", "id": "bar", "v": 1}
            ]

        If a non-existent key or ID is passed in, a series will be created
        for that key/ID and the data point written in to the new series.

        :param list data: a list of DataPoints to write
        :rtype: :class:`tempodb.response.Response` object"""

        url = 'multi/'

        dlist = [d.to_dictionary() for d in data]
        body = json.dumps(dlist)
        resp = self.session.post(url, body)
        return resp

    #INCREMENT METHODS
    #@with_response_type('Nothing')
    #def increment(self, key, data=[]):
    #    """Increment a series a data points by the specified amount.  For
    #    instance, incrementing with the following data::
#
    #        data = [{"t": "2012-01-08T00:21:54.000+0000", "v": 4.164}]
#
    #    would increment the value at that time by 4.
#
    #    **Note:** all floating point values are converted to longs before
    #    the increment takes place.
#
    #    :param string key: the series whose value to increment
    #    :param list data: the data points to incrememnt
    #    :rtype: :class:`tempodb.response.Response` object"""

    #    url = make_series_url(key)
    #    url = urlparse.urljoin(url + '/', 'increment')
    #    dlist = [d.to_dictionary() for d in data]
    #    body = json.dumps(dlist)
    #    resp = self.session.post(url, body)
    #    return resp

    #SINGLE VALUE METHODS
    @with_response_type('SingleValue')
    def single_value(self, key, ts=None, direction=None):
        """Return a single value for a series.  You can supply a timestamp
        as the ts argument, otherwise the search defaults to the current
        time.

        The dire`ction argument can be one of "exact", "before", "after", or
        "nearest".

        :param string key: the key for the series to use
        :param ts: (optional) the time to begin searching from
        :type ts: ISO8601 string or Datetime object
        :param string direction: criterion for the search
        :rtype: :class:`tempodb.response.Response` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'single')

        if ts is not None:
            vts = check_time_param(ts)
        else:
            vts = None

        params = {
            'ts': vts,
            'direction': direction
        }

        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_response_type(['SingleValue'])
    def multi_series_single_value(self, key=None, ts=None, direction=None,
                                  attr={}, tag=[]):
        """Return a single value for multiple series.  You can supply a
        timestamp as the ts argument, otherwise the search defaults to the
        current time.

        The direction argument can be one of "exact", "before", "after", or
        "nearest".

        The id, key, tag, and attr arguments allow you to filter for series.
        See the :meth:`list_series` method for an explanation of their use.

        :param string key: (optional) a list of keys for the series to use
        :param ts: (optional) the time to begin searching from
        :type ts: ISO8601 string or Datetime object
        :param string direction: criterion for the search
        :param tag: filter by one or more tags
        :type tag: list or string
        :param dict attr: filter by one or more key-value attributes
        :rtype: :class:`tempodb.response.Response` object"""

        url = 'single/'
        if ts is not None:
            vts = check_time_param(ts)
        else:
            vts = None

        params = {
            'key': key,
            'tag': tag,
            'attr': attr,
            'ts': vts,
            'direction': direction
        }

        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_response_type('Nothing')
    def delete(self, key, start, end):
        """Deletes data in a given series over the timeframe specified
        by start and end.

        :param string key: a list of keys for the series to use
        :param start: the time to begin deleting from
        :type start: ISO8601 string or Datetime object
        :param end: the time to end deleting at
        :type end: ISO8601 string or Datetime object
        :rtype: :class:`tempodb.response.Response` object"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'data')
        vstart = check_time_param(start)
        vend = check_time_param(end)

        params = {
            'start': vstart,
            'end': vend
        }

        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.delete(url)
        return resp
