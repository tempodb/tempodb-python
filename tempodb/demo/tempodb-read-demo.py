"""
http://tempo-db.com/api/read-series/#read-series-by-key
"""

import datetime
from tempodb.client import Client

# Modify these with your settings found at: http://tempo-db.com/manage/
API_KEY = 'my-key'
API_SECRET = 'my-secret'
SERIES_KEY = 'stuff'

client = Client(API_KEY, API_SECRET)

start = datetime.date(2012, 1, 1)
end = start + datetime.timedelta(days=1)

cursor = client.read_data(SERIES_KEY, start, end)
print 'Response code:', cursor.response.status

for datapoint in cursor:
    print datapoint.t.isoformat(), ':',  datapoint.v
