import functools
import urlparse
import urllib
import json
import endpoint
import protocol
from response import Response, ResponseException
from temporal.validate import check_time_param


def make_series_url(key):
    """For internal use. Given a series key, generate a valid URL to the series
    endpoint for that key.

    :param string key: the series key
    :rtype: string"""

    url = urlparse.urljoin(endpoint.SERIES_ENDPOINT, 'key/')
    url = urlparse.urljoin(url, urllib.quote_plus(key))
    return url


class with_response_type(object):
    """For internal use. Decorator for ensuring the Response object returned by
    the :class:`Client` object has a data attribute that corresponds to the
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
            else:
                raise ResponseException(resp_obj)
            return resp_obj
        return wrapper


class with_cursor(object):
    """For internal use. Decorator class for automatically transforming a
    response into a Cursor of the required type.

    :param class cursor_type: the cursor class to use
    :param class data_type: the data type that cursor should generate"""

    def __init__(self, cursor_type, data_type):
        self.cursor_type = cursor_type
        self.data_type = data_type

    def __call__(self, f, *args, **kwargs):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            resp = f(*args, **kwargs)
            session = args[0].session
            resp_obj = Response(resp, session)
            if resp_obj.status == 200:
                data = json.loads(resp_obj.body)
                if self.cursor_type in [protocol.SeriesCursor,
                                        protocol.SingleValueCursor]:
                    return self.cursor_type(data, self.data_type, resp_obj)
                else:
                    return self.cursor_type(data, self.data_type, resp_obj,
                                            kwargs.get('tz'))
            raise ResponseException(resp_obj)
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
        * :meth:`read_multi`
        * :meth:`read_multi_rollups`
        * :meth:`get_summary`

    WRITING DATA

        * :meth:`write_data`
        * :meth:`write_multi`

    DELETING

        * :meth:`delete`

    SINGLE VALUE

        * :meth:`single_value`
        * :meth:`multi_series_single_value`

    :param string database_id: 32-character identifier for your database
    :param string key: your API key, currently the same as database_id
    :param string secret: your API secret"""

    def __init__(self, database_id, key, secret, base_url=endpoint.BASE_URL):
        self.database_id = database_id
        self.session = endpoint.HTTPEndpoint(database_id, key, secret,
                                             base_url)

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
    def delete_series(self, keys=None, tags=None, attrs=None,
                      allow_truncation=False):
        """Delete a series according to the given criteria.

        **Note:** for the key argument, the filter will return the *union* of
        those values.  For the tag and attr arguments, the filter will return
        the *intersection* of those values.

        :param keys: filter by one or more series keys
        :type keys: list or string
        :param tags: filter by one or more tags
        :type tags: list or string
        :param dict attrs: filter by one or more key-value attributes
        :param bool allow_truncation: whether to allow full deletion of a
                                      database. Default is False.
        :rtype: :class:`tempodb.response.Response` object"""

        params = {
            'key': keys,
            'tag': tags,
            'attr': attrs,
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
        :rtype: :class:`tempodb.response.Response` with a
                :class:`tempodb.protocol.objects.Series` data payload"""

        url = make_series_url(key)
        resp = self.session.get(url)
        return resp

    @with_cursor(protocol.SeriesCursor, protocol.Series)
    def list_series(self, keys=None, tags=None, attrs=None,
                    limit=1000):
        """Get a list of all series matching the given criteria.

        **Note:** for the key argument, the filter will return the *union* of
        those values.  For the tag and attr arguments, the filter will return
        the *intersection* of those values.

        :param keys: filter by one or more series keys
        :type keys: list or string
        :param tags: filter by one or more tags
        :type tags: list or string
        :param dict attrs: filter by one or more key-value attributes
        :rtype: :class:`tempodb.protocol.cursor.SeriesCursor` with an
                iterator over :class:`tempodb.protocol.objects.Series`
                objects"""

        params = {
            'key': keys,
            'tag': tags,
            'attr': attrs,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([endpoint.SERIES_ENDPOINT, url_args])
        resp = self.session.get(url)
        return resp

    @with_response_type('Series')
    def update_series(self, series):
        """Update a series with new attributes.  This does not change
        any of the data written to this series. The recommended workflow for
        series updates is to pull a Series object down using the
        :meth:`get_series` method, change its attributes, then pass it into
        this method.

        :param series: the series to update
        :type series: `tempodb.protocol.Series` object
        :rtype: :class:`tempodb.response.Response` object with the updated
                :class:`tempodb.protocol.objects.Series` as the data payload"""

        url = make_series_url(series.key)

        resp = self.session.put(url, series.to_json())
        return resp

    #DATA READING METHODS
    @with_cursor(protocol.DataPointCursor, protocol.DataPoint)
    def read_data(self, key, start=None, end=None, rollup=None,
                  period=None, interpolationf=None, interpolation_period=None,
                  tz=None, limit=1000):
        """Read data from a series given its ID or key.  Start and end times
        must be supplied.  They can either be ISO8601 encoded strings (i.e.
        2012-01-08T00:21:54.000+0000) or Python Datetime objects, which will
        be converted for you.

        The rollup parameter is optional and can include string values such
        as "sum" and "avg".  Below is a list of valid rollup functions:

            * count
            * sum
            * mult
            * min
            * max
            * stddev
            * ss
            * range
            * percentile,N (where N is what percentile to calculate)

        This will apply a rollup function to your raw dataset.  The
        optional period parameter will downsample your data according to the
        given resolution ("1min", "2day", etc).

        The optional interpolation parameters can be used to resample your
        data to a regular interval interpolation_period according to an
        interpolation function interpolationf. Valid values for
        interpolation_period are the same as for the period parameter, and
        valid values for interpolationf include "zoh" and "linear".

        Finally, the optional tz parameter can be used to specify a time zone
        for your output.  Please see
        `here <https://tempo-db.com/docs/api/timezone/>`_ for a list of a
        valid timezone values.

        :param string key: the series key to use
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string rollup: (optional) the name of a rollup function to use
        :param string period: (optional) downsampling rate for the data
        :param string interpolationf: (optional) an interpolation function
                                      to run over the series
        :param string interpolation_period: (optional) the period to
                                            interpolate data into
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` with an
                iterator over :class:`tempodb.protocol.objects.DataPoint`
                objects"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'segment')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'rollup.fold': rollup,
            'rollup.period': period,
            'interpolation.function': interpolationf,
            'interpolation.period': interpolation_period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_response_type('SeriesSummary')
    def get_summary(self, key, start, end, tz=None):
        """Get a summary for the series from *start* to *end*.  The summary is
        a map containing keys *count*, *min*, *max*, *mean*, *sum*, and
        *stddev*.

        :param string key: the series key to use
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.response.Response` with a
                :class:`tempodb.protocol.objects.SeriesSummary` data payload"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'summary')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'tz': tz
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_cursor(protocol.DataPointCursor, protocol.MultiPoint)
    def read_multi_rollups(self, key, start, end, rollups, period,
                           tz=None, interpolationf=None,
                           interpolation_period=None, limit=5000):
        """Read data from a single series with multiple rollups applied.
        The rollups parameter should be a list of rollup names.

        :param string key: the series key to read from
        :param list rollups: the rollup functions to use
        :param list keys: (optional) filter by one or more series keys
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string period: (optional) downsampling rate for the data
        :param string tz: (optional) the timezone to place the data into
        :param string interpolationf: (optional) an interpolation function
                                      to run over the series
        :param string interpolation_period: (optional) the period to
                                            interpolate data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` with an
                iterator over :class:`tempodb.protocol.objects.MultiPoint`
                objects"""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'data/rollups/segment')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'limit': limit,
            'rollup.fold': rollups,
            'rollup.period': period,
            'interpolation.function': interpolationf,
            'interpolation.period': interpolation_period,
            'tz': tz
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_cursor(protocol.DataPointCursor, protocol.DataPointFound)
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
        :param string period: downsampling rate for the data
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` with an
                iterator over :class:`tempodb.protocol.objects.DataPointFound`
                objects."""

        url = make_series_url(key)
        url = urlparse.urljoin(url + '/', 'find')

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'start': vstart,
            'end': vend,
            'predicate.function': predicate,
            'predicate.period': period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_cursor(protocol.DataPointCursor, protocol.DataPoint)
    def aggregate_data(self, start, end, aggregation, keys=[], tags=[],
                       attrs={}, rollup=None, period=None, interpolationf=None,
                       interpolation_period=None, tz=None, limit=1000):
        """Read data from multiple series according to a filter and apply a
        function across all the returned series to put the datapoints together
        into one aggregrate series.

        See the :meth:`list_series` method for a description of how the filter
        criteria are applied, and the :meth:`read_data` method for how to
        work with the start, end, and tz parameters.

        Valid aggregation functions are the same as valid rollup functions.

        :param string aggregation: the aggregation to perform
        :param keys: (optional) filter by one or more series keys
        :type keys: list or string
        :param tags: (optional) filter by one or more tags
        :type tags: list or string
        :param dict attrs: (optional) filter by one or more key-value
                           attributes
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string rollup: (optional) the name of a rollup function to use
        :param string period: (optional) downsampling rate for the data
        :param string interpolationf: (optional) an interpolation function
                                      to run over the series
        :param string interpolation_period: (optional) the period to
                                            interpolate data into
        :param string tz: (optional) the timezone to place the data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` with an
                iterator over :class:`tempodb.protocol.objects.DataPoint`
                objects"""

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
            'rollup.fold': rollup,
            'rollup.period': period,
            'interpolation.function': interpolationf,
            'interpolation.period': interpolation_period,
            'tz': tz,
            'limit': limit
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

    @with_cursor(protocol.DataPointCursor, protocol.MultiPoint)
    def read_multi(self, start, end, keys=None, rollup=None, period=None,
                   tz=None, tags=None, attrs=None, interpolationf=None,
                   interpolation_period=None, limit=5000):
        """Read data from multiple series given filter criteria.  See the
        :meth:`list_series` method for a description of how the filter
        criteria are applied, and the :meth:`read_data` method for how to
        work with the start, end, function, interval, and tz parameters.

        :param keys: (optional) filter by one or more series keys
        :type keys: list or string
        :param tags: filter by one or more tags
        :type tags: list or string
        :param dict attrs: (optional) filter by one or more key-value
                            attributes
        :param start: the start time for the data points
        :type start: string or Datetime
        :param end: the end time for the data points
        :type end: string or Datetime
        :param string rollup: (optional) the name of a rollup function to use
        :param string period: (optional) downsampling rate for the data
        :param string tz: (optional) the timezone to place the data into
        :param string interpolationf: (optional) an interpolation function
                                      to run over the series
        :param string interpolation_period: (optional) the period to
                                            interpolate data into
        :rtype: :class:`tempodb.protocol.cursor.DataPointCursor` with an
                iterator over :class:`tempodb.protocol.objects.MultiPoint`
                objects"""

        url = 'multi'

        vstart = check_time_param(start)
        vend = check_time_param(end)
        params = {
            'key': keys,
            'tag': tags,
            'attr': attrs,
            'start': vstart,
            'end': vend,
            'limit': limit,
            'rollup.fold': rollup,
            'rollup.period': period,
            'interpolation.function': interpolationf,
            'interpolation.period': interpolation_period,
            'tz': tz
        }
        url_args = endpoint.make_url_args(params)
        url = '?'.join([url, url_args])
        resp = self.session.get(url)
        return resp

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

        The direction argument can be one of "exact", "before", "after", or
        "nearest".

        :param string key: the key for the series to use
        :param ts: (optional) the time to begin searching from
        :type ts: ISO8601 string or Datetime object
        :param string direction: criterion for the search
        :rtype: :class:`tempodb.response.Response` with a
                :class:`tempodb.protocol.objects.SingleValue` object as the
                data payload"""

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

    @with_cursor(protocol.SingleValueCursor, protocol.SingleValue)
    def multi_series_single_value(self, keys=None, ts=None, direction=None,
                                  attrs={}, tags=[]):
        """Return a single value for multiple series.  You can supply a
        timestamp as the ts argument, otherwise the search defaults to the
        current time.

        The direction argument can be one of "exact", "before", "after", or
        "nearest".

        The id, key, tag, and attr arguments allow you to filter for series.
        See the :meth:`list_series` method for an explanation of their use.

        :param string keys: (optional) a list of keys for the series to use
        :param ts: (optional) the time to begin searching from
        :type ts: ISO8601 string or Datetime object
        :param string direction: criterion for the search
        :param tags: filter by one or more tags
        :type tags: list or string
        :param dict attrs: filter by one or more key-value attributes
        :rtype: :class:`tempodb.protocol.cursor.SingleValueCursor` with an
                iterator over :class:`tempodb.protocol.objects.SingleValue`
                objects"""

        url = 'single/'
        if ts is not None:
            vts = check_time_param(ts)
        else:
            vts = None

        params = {
            'key': keys,
            'tag': tags,
            'attr': attrs,
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
