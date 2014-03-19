Examples and Snippets
=====================

Below are some examples to help you get started using the API.

Setting up a client
-------------------

To set up a client, simply use your database ID, API key, and secret::

    from tempodb.client import Client


    DATABASE_ID = 'my database'
    API_KEY = DATABASE_ID    # Currently API_KEY is the same as DATABASE_ID
    API_SECRET = 'my secret'

    client = Client(DATABASE_ID, API_KEY, API_SECRET)

Creating a series
-----------------

Series are created by key.  Additionally, if you attempt to write to a 
non-existent series, the series will be automatically created for you::

    client.create_series('my-series')

Modifying a series
------------------

Series have several attributes that can be modified - name, tags, and 
attributes.  The recommended way to update a series is using the standard
read-update-write cycle::

    response = client.get_series('my-key')
    series1 = response.data
    series1.name = 'foobar'
    series1.tags = ['baz', 'abc']
    series1.attributes = {'foo': 'bar'}
    client.update_series(series1)

Writing data
------------

There are two options for writing data - writing to one series at a time or 
writing to multiple series in one shot.

Writing to one series::

    import datetime
    import random


    series = 'my-series'
    data = []
    date = datetime.datetime(2012, 1, 1)

    #writing random data
    for minute in range(1, 1441):

        dp = DataPoint.from_data(date, random.random() * 100.0)
        data.append(dp)
        date = date + datetime.timedelta(minutes=1)
    
    client.write_data(series, data)

Writing to multiple series::

    series = ['series1', 'series2'] 
    for minute in range(1, 1441):
        #choose a series randomly to write to - note that you add a key 
        #argument to tell the API which series the data point belongs to
        dp = DataPoint.from_data(date, random.random() * 100.0,
                                 key=random.choice(series))
        data.append(dp)
        date = date + datetime.timedelta(minutes=1)
    
    client.write_multi(series, data)

Reading data
------------

Once data has been written to a series, reading it back out is 
straightforward::

    start = datetime.date(2012, 1, 1)
    end = start + datetime.timedelta(days=1)
    response = client.read_data('my-series', start, end)

    for d in response.data:
        # do something

Note that your actual data points are stored in a cursor.  This cursor will 
automatically handle the API's pagination until it reaches the end of the data 
you request.  It is also "read once."  After you have iterated through a 
cursor, accessing the data again will require another API call.


