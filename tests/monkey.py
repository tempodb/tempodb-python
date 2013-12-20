import mock


def monkeypatch_requests(end):
    setattr(end.pool, 'get', mock.Mock())
    setattr(end.pool, 'post', mock.Mock())
    setattr(end.pool, 'put', mock.Mock())
    setattr(end.pool, 'delete', mock.Mock())
