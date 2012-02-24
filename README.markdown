# TempoDB Python API Client

The TempoDB Python API Client makes calls to the [TempoDB API](http://tempo-db.com/api/).  The module is available on PyPi as [tempodb](http://pypi.python.org/pypi/tempodb/).

1. Install tempodb

``
pip install tempodb
``

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

## Available functions

### create_database(*name*)

### get_series()

### read_id(*series_id*, *start*, *end*, *interval (optional)*, *function (optional*)

### read_key(*series_key*, *start*, *end*, *interval (optional)*, *function (optional*)

### write_id(*series_id*, *data*)

### write_key(*series_key*, *data*)

### write_bulk(*data*)