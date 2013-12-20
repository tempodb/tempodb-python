import unittest
import json
import mock
from tempodb.protocol.cursor import Cursor, DataPointCursor, SeriesCursor


class DummyType(object):
    def __init__(self, data, response, tz=None):
        self.data = data
        self.response = response
        self.tz = tz


class Dummy(object):
    pass


class DummyResponse(object):
    def __init__(self):
        self.resp = Dummy()
        self.resp.headers = {}
        self.headers = {}
        self.text = ''
        self.session = mock.Mock()
        self.status_code = 200
        self.reason = 'OK'
        self.links = {}


class TestCursor(unittest.TestCase):
    def test_cursor_constructor(self):
        c = Cursor([1, 2, 3], DummyType, None)
        self.assertEquals(c.type, DummyType)
        self.assertEquals(c.response, None)

    def test_cursor_iterator(self):
        c = Cursor([1, 2, 3], DummyType, None)
        d = [i for i in c]
        self.assertEquals(len(d), 3)

    def test_data_point_cursor_iterator(self):
        second = json.dumps({'data': [4, 5, 6]})
        resp = DummyResponse()
        secondary_response = DummyResponse()
        secondary_response.text = second
        secondary_response.resp.links = {}
        resp.session.get.return_value = secondary_response
        resp.resp.links = {'next': {'url': '<...>'}}
        c = DataPointCursor([1, 2, 3], DummyType, resp)
        d = [i for i in c]
        resp.session.get.assert_called_once_with('<...>')
        self.assertEquals(len(d), 6)

    def test_data_point_cursor_iterator_stops_at_invalid_link(self):
        second = json.dumps({'data': [4, 5, 6]})
        resp = DummyResponse()
        secondary_response = DummyResponse()
        secondary_response.text = second
        secondary_response.status_code = 403
        secondary_response.resp.links = {}
        resp.session.get.return_value = secondary_response
        resp.resp.links = {'next': {'url': 'foo'}}
        c = DataPointCursor([1, 2, 3], DummyType, resp)
        got_value_error = False
        try:
            [i for i in c]
        except ValueError:
            got_value_error = True
        self.assertTrue(got_value_error)

    def test_series_cursor_iterator(self):
        second = json.dumps([4, 5, 6])
        resp = DummyResponse()
        secondary_response = DummyResponse()
        secondary_response.text = second
        secondary_response.resp.links = {}
        resp.session.get.return_value = secondary_response
        resp.resp.links = {'next': {'url': '<...>'}}
        c = SeriesCursor([1, 2, 3], DummyType, resp)
        d = [i for i in c]
        resp.session.get.assert_called_once_with('<...>')
        self.assertEquals(len(d), 6)

    def test_series_cursor_iterator_stops_at_invalid_link(self):
        second = json.dumps([4, 5, 6])
        resp = DummyResponse()
        secondary_response = DummyResponse()
        secondary_response.text = second
        secondary_response.status_code = 403
        secondary_response.resp.links = {}
        resp.session.get.return_value = secondary_response
        resp.resp.links = {'next': {'url': 'foo'}}
        c = SeriesCursor([1, 2, 3], DummyType, resp)
        got_value_error = False
        try:
            [i for i in c]
        except ValueError:
            got_value_error = True
        self.assertTrue(got_value_error)
