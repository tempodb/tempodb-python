"""
http://tempo-db.com/api/read-series/#read-series-by-key
"""

import datetime
from tempodb.client import Client

client = Client('your-api-key', 'your-api-secret')

start = datetime.date(2012, 1, 1)
end = start + datetime.timedelta(days=1)

data = client.read_key('your-custom-key', start, end)

for datapoint in data:
    print datapoint
