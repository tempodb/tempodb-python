import unittest
from tempodb.response import Response
from test_protocol_cursor import DummyResponse


class TestResponse(unittest.TestCase):
    def test_response_constructor_success(self):
        resp = DummyResponse()
        r = Response(resp, None)
        self.assertEquals(r.status, 200)
        self.assertEquals(r.reason, 'OK')
        self.assertEquals(r.successful, 0)
        self.assertEquals(r.error, None)

    def test_response_constructor_failure(self):
        resp = DummyResponse()
        resp.status_code = 403
        resp.reason = 'Forbidden'
        resp.text = 'foo'
        r = Response(resp, None)
        self.assertEquals(r.status, 403)
        self.assertEquals(r.reason, 'Forbidden')
        self.assertEquals(r.successful, 1)
        self.assertEquals(r.error, 'foo')

    def test_response_constructor_partial(self):
        resp = DummyResponse()
        resp.status_code = 207
        resp.reason = 'Forbidden'
        resp.text = 'foo'
        r = Response(resp, None)
        self.assertEquals(r.status, 207)
        self.assertEquals(r.reason, 'Forbidden')
        self.assertEquals(r.successful, 2)
        self.assertEquals(r.error, 'foo')
