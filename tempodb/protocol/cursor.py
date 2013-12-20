import json


def make_generator(d):
    """"Utility function for converting a list to a generator.

    :param list d: the list to convert
    :rtype: generator"""

    for i in d:
        yield i


def check_response(resp):
    """Utility function for checking the status of a cursor increment.  Raises
    an exception if the call to the paginated link returns anything other than
    a 200.

    :param resp: the response to check
    :type resp: :class:`tempodb.response.Response` object
    :raises ValueError: if the response is not a 200
    :rtype: None"""

    s = resp.status
    if s != 200:
        raise ValueError(
            'TempoDB API returned %d as status when 200 was expected' % s)


class Cursor(object):
    """An iterable cursor over data retrieved from the TempoDB API.  The
    cursor will make network requests to fetch more data as needed, until
    the API returns no more data.  It can be used with the standard
    iterable interface:

        >>> data = [d for d in response.data.cursor]"""

    def __init__(self, data, t, response):
        self.response = response
        self.type = t
        self.data = make_generator(
            [self.type(d, self.response) for d in data])

    def __iter__(self):
        while True:
            try:
                x = self.data.next()
                yield x
            except StopIteration:
                self._fetch_next()

    def _fetch_next(self):
        raise StopIteration


class DataPointCursor(Cursor):
    def __init__(self, data, t, response, tz=None):
        self.response = response
        self.type = t
        self.tz = tz
        self.data = make_generator(
            [self.type(d, self.response, tz=tz) for d in data])

    def _fetch_next(self):
        try:
            link = self.response.resp.links['next']['url']
        except KeyError:
            raise StopIteration

        n = self.response.session.get(link)
        #HACK: put here to avoid circular import, no performance hit
        #because the VM will cache the module
        from tempodb.response import Response
        self.response = Response(n, self.response.session)
        check_response(self.response)
        j = json.loads(n.text)
        self.data = make_generator(
            [self.type(d, self.response, tz=self.tz) for d in j['data']])


class SeriesCursor(Cursor):
    def _fetch_next(self):
        try:
            link = self.response.resp.links['next']['url']
        except KeyError:
            raise StopIteration

        n = self.response.session.get(link)
        from tempodb.response import Response
        self.response = Response(n, self.response.session)
        check_response(self.response)
        j = json.loads(n.text)
        self.data = make_generator(
            [self.type(d, self.response) for d in j])
