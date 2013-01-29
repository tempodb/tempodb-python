"""
http://tempo-db.com/api/write-series/#bulk-write-multiple-series
"""

import datetime
from tempodb import Client

# Modify these with your credentials found at: http://tempo-db.com/manage/
API_KEY = 'your-api-key'
API_SECRET = 'your-api-secret'
SERIES_KEY = 'your-custom-key'

client = Client(API_KEY, API_SECRET)

ts = datetime.datetime.now()
data = [
    { 'key': 'custom-series-key1', 'v': 1.11 },
    { 'key': 'custom-series-key2', 'v': 2.22 },
    { 'key': 'custom-series-key3', 'v': 3.33 },
    { 'key': 'custom-series-key4', 'v': 4.44 },
]

print client.write_bulk(ts, data)
