"""
http://tempo-db.com/api/read-series/#read-series-by-key
"""

import datetime
from tempodb import Client

client = Client('your-api-key', 'your-api-secret')

now = datetime.datetime.now()

start = datetime.datetime.now().date()
end = start + datetime.timedelta(days=1)

data = client.read_key('some-key', start, end)

for datapoint in data:
    print datapoint