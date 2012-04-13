"""
http://tempo-db.com/api/write-series/#write-series-by-key
"""

import datetime
import random
from tempodb import Client, DataPoint

client = Client('your-api-key', 'your-api-secret')

date = datetime.datetime(2012, 1, 1)

for day in range(1, 10):
    # print out the current day we are sending data for
    print date

    data = []
    # 1440 minutes in one day
    for min in range (1, 1441):
        data.append(DataPoint(date, random.random() * 50.0))
        date = date + datetime.timedelta(minutes=1)

    client.write_key('your-custom-key', data)
