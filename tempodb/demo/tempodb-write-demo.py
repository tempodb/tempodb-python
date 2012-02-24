"""
http://tempo-db.com/api/write-series/#write-series-by-key
"""

import datetime
import random
from tempodb import Client

client = Client('your-api-key', 'your-api-secret')

now = datetime.datetime.now()
data = [
    { 't': now.isoformat(), 'v': 77.77, },
    { 't': (now+datetime.timedelta(minutes=1)).isoformat(), 'v': 42.17, },
    { 't': (now+datetime.timedelta(minutes=2)).isoformat(), 'v': 0, },
    { 't': (now+datetime.timedelta(minutes=3)).isoformat(), 'v': 6.8873, },
]

date = datetime.datetime(2012, 2, 8)

for day in range(1, 10):
    print date

    data = []
    # 1440 minutes in one day
    for min in range (1, 1441):
        data.append({
            't': date.isoformat(),
            'v': random.random() * 50.0
        })
        date = date + datetime.timedelta(minutes=1)

    client.write_key('some-key', data)
