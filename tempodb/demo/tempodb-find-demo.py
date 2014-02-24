"""
http://tempo-db.com/api/read-series/#read-series-by-key
"""

import datetime
from tempodb.client import Client

# Modify these with your settings found at: http://tempo-db.com/manage/
DATABASE_ID = 'my-id'
API_KEY = 'my-key'
API_SECRET = 'my-secret'
SERIES_KEY = 'stuff'

client = Client(DATABASE_ID, API_KEY, API_SECRET)

start = datetime.date(2012, 1, 1)
end = start + datetime.timedelta(days=1)

cursor = client.find_data(SERIES_KEY, start, end, 'max', 'PT1H')
print 'Response code:', cursor.response.status

fmt = '(%s - %s): %0.2f at %s'
for datapoint in cursor:
    print fmt % (datapoint.start.isoformat(),
                 datapoint.end.isoformat(),
                 datapoint.v, datapoint.t)
