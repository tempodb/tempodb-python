"""
http://tempo-db.com/api/write-series/#write-series-by-key
"""

import datetime
import random
from tempodb.client import Client
from tempodb.protocol import DataPoint

# Modify these with your credentials found at: http://tempo-db.com/manage/
API_KEY = 'my-key'
API_SECRET = 'my-secret'
SERIES_KEY = 'stuff'

client = Client(API_KEY, API_SECRET)

date = datetime.datetime(2012, 1, 1)

for day in range(1, 10):
    # print out the current day we are sending data for
    print date

    data = []
    # 1440 minutes in one day
    for min in range(1, 1441):
        data.append(DataPoint.from_data(date, random.random() * 50.0))
        date = date + datetime.timedelta(minutes=1)

    resp = client.write_data(SERIES_KEY, data)
    print 'Response code:', resp.status

    if resp.status != 200:
        print 'Error reason:', resp.error
