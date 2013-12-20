import unittest
from tempodb.protocol.protocol import make_series_key


class TestProtocolBasics(unittest.TestCase):
    def test_make_series_key(self):
        k = 'my-key'
        tags = ['foo']
        attrs = {'bar': 'baz'}
        ret = make_series_key(k, tags, attrs)
        self.assertEquals(ret, '{"attributes": {"bar": "baz"}, "key": "my-key", "tags": ["foo"]}')
