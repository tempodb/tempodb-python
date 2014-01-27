import unittest
from monkey import monkeypatch_requests
from tempodb import endpoint as p


class TestEndpoint(unittest.TestCase):
    def setUp(self):
        self.end = p.HTTPEndpoint('my_id', 'foo', 'bar',
                                  'http://www.nothing.com')
        monkeypatch_requests(self.end)

    def test_make_url_args_list(self):
        params = {'foo': [1, 'foo']}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=1&foo=foo')

    def test_make_url_args_tuple(self):
        params = {'foo': (1, 'foo')}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=1&foo=foo')

    def test_make_url_args_dict(self):
        params = {'foo': {'bar': 'baz',
                          'abc': 'def'}}
        ret = p.make_url_args(params)
        pass1 = 'foo%5Bbar%5D=baz' in ret
        pass2 = 'foo%5Babc%5D=def' in ret
        self.assertTrue(pass1)
        self.assertTrue(pass2)

    def test_make_url_args_bool_true(self):
        params = {'foo': True}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=true')

    def test_make_url_args_bool_false(self):
        params = {'foo': False}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=false')

    def test_make_url_args_bool_none(self):
        params = {'foo': None}
        ret = p.make_url_args(params)
        self.assertEquals(ret, '')

    def test_make_url_args_bool_bare_value_str(self):
        params = {'foo': 'bar'}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=bar')

    def test_make_url_args_bool_bare_value_non_str(self):
        params = {'foo': 2.0}
        ret = p.make_url_args(params)
        self.assertEquals(ret, 'foo=2.0')

    def test_endpoint_constructor(self):
        self.assertEquals(self.end.base_url, 'http://www.nothing.com/')
        self.assertEquals(self.end.headers['User-Agent'],
                          'tempo-db-python-test/%s' % 0.1)
        self.assertEquals(self.end.headers['Accept-Encoding'], 'gzip')
        self.assertTrue(hasattr(self.end, 'auth'))

    def test_endpoint_constructor_with_slash(self):
        self.end = p.HTTPEndpoint('my_id', 'foo', 'bar',
                                  'http://www.nothing.com/')
        self.assertEquals(self.end.base_url, 'http://www.nothing.com/')
        self.assertEquals(self.end.headers['User-Agent'],
                          'tempo-db-python-test/%s' % 0.1)
        self.assertEquals(self.end.headers['Accept-Encoding'], 'gzip')
        self.assertTrue(hasattr(self.end, 'auth'))

    def test_endpoint_post(self):
        url = 'series/'
        body = 'foobar'
        self.end.pool.post.return_value = True
        self.end.post(url, body)
        self.end.pool.post.assert_called_once_with(
            'http://www.nothing.com/series/',
            data=body,
            auth=self.end.auth)

    def test_endpoint_get(self):
        url = 'series/'
        self.end.pool.get.return_value = True
        self.end.get(url)
        self.end.pool.get.assert_called_once_with(
            'http://www.nothing.com/series/',
            auth=self.end.auth)

    def test_endpoint_put(self):
        url = 'series/'
        body = 'foobar'
        self.end.pool.put.return_value = True
        self.end.put(url, body)
        self.end.pool.put.assert_called_once_with(
            'http://www.nothing.com/series/',
            data=body,
            auth=self.end.auth)

    def test_endpoint_delete(self):
        url = 'series/'
        self.end.pool.delete.return_value = True
        self.end.delete(url)
        self.end.pool.delete.assert_called_once_with(
            'http://www.nothing.com/series/',
            auth=self.end.auth)
