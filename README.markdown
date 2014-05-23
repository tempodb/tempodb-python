# TempoDB Python API Client

[![Build Status](https://secure.travis-ci.org/tempodb/tempodb-python.png)](http://travis-ci.org/tempodb/tempodb-python)

The TempoDB Python API Client makes calls to the [TempoDB API](http://tempo-db.com/api/).  The module is available on PyPi as [tempodb](http://pypi.python.org/pypi/tempodb/).

Install tempodb from PyPI

``
pip install tempodb
``

Install tempodb from [source](https://github.com/tempodb/tempodb-python/)

``
git clone https://github.com/tempodb/tempodb-python.git
``

``
cd tempodb-python
``

``
python setup.py install
``

Run unit tests
``
cd path/to/tempodb-python
``

``
python setup.py nosetests
``

Build documentation - if built from source and would like a local copy
``
cd path/to/tempodb-python/docs
``

``
make html
``

``
cd build
``

``
firefox index.html
``

1. Examples can be found in documentation

1. After installing tempodb, download [tempodb-write-demo.py](https://github.com/tempodb/tempodb-python/blob/master/tempodb/demo/tempodb-write-demo.py).

1. Edit *your-api-key* and *your-api-secret* in tempodb-write-demo.py.

1. Run tempodb-write-demo.py to insert 10 days of test data.

``
python tempodb-write-demo.py
``

1. Download [tempodb-read-demo.py](https://github.com/tempodb/tempodb-python/blob/master/tempodb/demo/tempodb-read-demo.py)

1. Edit *your-api-key* and *your-api-secret* in tempodb-read-demo.py.

1. Run tempodb-read-demo.py to read back the data you just wrote in.

``
python tempodb-read-demo.py
``


