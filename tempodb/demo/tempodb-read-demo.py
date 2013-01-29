"""
http://tempo-db.com/api/read-series/#read-series-by-key
"""

import datetime
from tempodb import Client

# Modify these with your settings found at: http://tempo-db.com/manage/
API_KEY = 'your-api-key'
API_SECRET = 'your-api-secret'
SERIES_KEY = 'your-custom-key'

client = Client(API_KEY, API_SECRET)

start = datetime.date(2012, 1, 1)
end = start + datetime.timedelta(days=1)

data = client.read_key(SERIES_KEY, start, end)

for datapoint in data.data:
    print datapoint
