import json
from tempodb.temporal.validate import convert_iso_stamp, check_time_param
from cursor import DataPointCursor, SeriesCursor, SingleValueCursor


class JSONSerializable(object):
    """Base class for objects that are serializable to and from JSON.
    This class defines default methods for serializing each way that use
    the class's "properties" class variable to determine what should be
    serialized or deserialized.  For example::

        class MySerialized(JSONSerializable)
            properties = ['foo', 'bar']

    This would define a class that expects to have the 'foo' and 'bar'
    keys in JSON data and would likewise serialize a JSON object with
    those keys.

    The base constructor calls the :meth:`from_json` method, which
    enforces these constraints for object construction.  If you override
    this constructor (for example, to provide static initialization of
    some variables), it is highly recommended that the subclass constructor
    call this constructor at some point through super().

    :param string json_text: the JSON string to deserialize from"""

    properties = []

    def __init__(self, json_text, response):
        self.from_json(json_text)
        self.response = response

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        #due to the architecture of response parsing, particularly
        #where the API returns lists, the JSON might have already been
        #parsed by the time it gets here
        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                setattr(self, p, j[p])
        except KeyError, e:
            msg = 'Expected key %s in JSON object, found None' % str(e)
            raise ValueError(msg)

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            j[p] = getattr(self, p)

        return json.dumps(j)

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        for p in self.properties:
            j[p] = getattr(self, p)

        return j


#PLACEHOLDER FOR EMPTY RESPONSES
class Nothing(object):
    """Used to represent empty responses.  This class should not be
    used directly in user code."""

    def __init__(self, *args, **kwargs):
        pass


class SeriesSet(object):
    """Represents a set of Series objects as returned by the list series
    TempoDB API endpoint.  The SeriesSet exposes a cursor that can be iterated
    over to examine each series return by the API."""

    def __init__(self, json_text, response):
        self.data = json.loads(json_text)
        self.cursor = SeriesCursor(self.data, Series, response)


class Series(JSONSerializable):
    """Represents a Series object from the TempoDB API.  Series objects
    are serialized to and from JSON using the :meth:`to_json` and
    :meth:`from_json` methods.

    Domain object attributes:

        * key: string
        * name: string
        * tags: list
        * attributes: dictionary"""

    properties = ['key', 'name', 'tags', 'attributes']

    def __init__(self, json_text, response):
        #the formatting of the series object returned from the series by key
        #endpoint is slightly different
        if isinstance(json_text, basestring):
            j = json.loads(json_text)
        else:
            j = json_text
        if 'series' in j:
            self.from_json(j['series'])
        else:
            self.from_json(json_text)
        self.response = response


class DataSet(JSONSerializable):
    """Represents a data set returned using the /data resource in the
    TempoDB API.  Depending on the original API call, some attributes of
    this object (such as rollup) could be None."""

    properties = ['data', 'rollup', 'tz']

    def __init__(self, json_text, response):
        #override to force the instantiation of a cursor
        super(DataSet, self).__init__(json_text, response)
        self.cursor = DataPointCursor(self.data, DataPoint, response)
        if self.rollup is not None:
            self.rollup = Rollup(self.rollup)


class SingleValue(JSONSerializable):
    """Represents a data set returned by calling the single value
    endpoint of the TempoDB API.  This domain object is not cursored, so
    it is implemented separately from the more generic DataSet object.

    Domain object attributes:

        * series: :class:`Series` object
        * data: :class:`DataPoint` object"""

    properties = ['series', 'data']

    def __init__(self, json_text, response):
        #force conversion of the subobjects in this datatype after we get
        #them
        super(SingleValue, self).__init__(json_text, response)
        self.series = Series(self.series, response)
        if self.data is not None:
            self.data = DataPoint(self.data, response, self.data.get('tz'))

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        j['series'] = self.series.to_dictionary()
        j['data'] = self.data.to_dictionary()
        return j

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        return json.dumps(self.to_dictionary())


class SeriesSummary(JSONSerializable):
    properties = ['series', 'summary', 'tz', 'start', 'end']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(SeriesSummary, self).__init__(json_text, response)
        self.series = Series(self.series, response)
        self.summary = Summary(self.summary, response)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p in ['start', 'end']:
                    val = convert_iso_stamp(j[p], self.tz)
                    setattr(self, p, val)
                else:
                    setattr(self, p, j[p])
        except KeyError:
            pass

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        return json.dumps(self.to_dictionary())

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        d = {'start': self.start.isoformat(),
             'end': self.end.isoformat(),
             'tz': self.tz,
             'summary': self.summary.to_dictionary(),
             'series': self.series.to_dictionary()
             }
        return d


class Summary(JSONSerializable):
    """Represents the summary received from the TempoDB API when a data read
    request is sent.  The properties are summary statistics for the dataset
    returned."""

    properties = ['mean', 'sum', 'min', 'max', 'stddev', 'count']


class Rollup(JSONSerializable):
    """Represents the rollup information returned from the TempoDB API when
    the API calls demands it."""

    properties = ['interval', 'function', 'tz']


class DataPoint(JSONSerializable):
    """Represents a single data point in a series.  To construct these objects
    in user code, use the class method :meth:`from_data`.

    Domain object attributes:

        * t: DateTime object
        * v: int or float
        * key: string (only present when writing DataPoints)
        * id: string (only present when writing DataPoints)"""

    properties = ['t', 'v', 'key', 'id']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(DataPoint, self).__init__(json_text, response)

    @classmethod
    def from_data(self, time, value, series_id=None, key=None, tz=None):
        """Create a DataPoint object from data, rather than a JSON object or
        string.  This should be used by user code to construct DataPoints from
        Python-based data like Datetime objects and floats.

        The series_id and key arguments are only necessary if you are doing a
        multi write, in which case those arguments can be used to specify which
        series the DataPoint belongs to.

        If needed, the tz argument should be an Olsen database compliant string
        specifying the time zone for this DataPoint.  This argument is most
        often used internally when reading data from TempoDB.

        :param time: the point in time for this reading
        :type time: ISO8601 string or Datetime
        :param value: the value for this reading
        :type value: int or float
        :param string series_id: (optional) a series ID for this point
        :param string key: (optional) a key for this point
        :param string tz: (optional) a timezone for this point
        :rtype: :class:`DataPoint`"""

        t = check_time_param(time)
        if type(value) in [float, int]:
            v = value
        else:
            raise ValueError('Values must be int or float. Got "%s".' %
                             str(value))

        j = {
            't': t,
            'v': v,
            'id': series_id,
            'key': key
        }
        return DataPoint(j, None, tz=tz)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p == 't':
                    val = convert_iso_stamp(j[p], self.tz)
                    setattr(self, p, val)
                else:
                    setattr(self, p, j[p])
        #overriding this exception allows us to handle optional values like
        #id and key which are only present during particular API calls like
        #multi writes
        except KeyError:
            pass

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            #this logic change allows us to work with optional values for
            #this data type
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return json.dumps(j)

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        for p in self.properties:
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return j


class DataPointFound(JSONSerializable):
    """Represents a specialized DataPoint returned by the the /find endpoint
    of the TempoDB API.  The start and end attributes indicate in what time
    period the datapoint was found, the t attribute indicates the exact time
    at which the point was found, and the v attribute indicates what the value
    of the point was at that time.

    Domain object attributes:

        * start: DateTime object
        * end: DateTime object
        * v: int or long
        * t: DateTime object"""

    properties = ['interval', 'found']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(DataPointFound, self).__init__(json_text, response)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p == 'interval':
                    self.start = convert_iso_stamp(j[p]['start'], self.tz)
                    self.end = convert_iso_stamp(j[p]['end'], self.tz)
                elif p == 'found':
                    t = convert_iso_stamp(j[p]['t'], self.tz)
                    setattr(self, 't', t)
                    v = j[p]['v']
                    setattr(self, 'v', v)
        #overriding this exception allows us to handle optional values like
        #id and key which are only present during particular API calls like
        #multi writes
        except KeyError:
            pass

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        j['interval'] = {'start': self.start.isoformat(),
                         'end': self.end.isoformat()}
        j['found'] = {'v': self.v, 't': self.t.isoformat()}
        return j

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        return json.dumps(self.to_dictionary())


class MultiPoint(JSONSerializable):
    """Represents a data point with values for multiple series at a single
    timestamp. Returned when performing a multi-series query.  The v attribute
    is a dictionary mapping series key to value.

    Domain object attributes:

        * t: DateTime object
        * v: dictionary"""

    properties = ['t', 'v']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(MultiPoint, self).__init__(json_text, response)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p == 't':
                    t = convert_iso_stamp(j[p], self.tz)
                    setattr(self, 't', t)
                else:
                    setattr(self, p, j[p])
        #overriding this exception allows us to handle optional values like
        #id and key which are only present during particular API calls like
        #multi writes
        except KeyError:
            pass

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return json.dumps(j)

    def get(self, k):
        """Convenience method for getting values for individual series out of
        the MultiPoint.  This is equivalent to calling::

            >>> point.v.get('foo')

        :param string k: the key to read
        :rtype: number"""

        return self.v.get(k)
