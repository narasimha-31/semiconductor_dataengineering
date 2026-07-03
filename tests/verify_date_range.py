import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CENSUS_API_KEY')
BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

test_dates = ['2010-01', '2013-01', '2015-01', '2018-01', '2020-01',
              '2022-01', '2024-01', '2025-01', '2025-12', '2026-01', '2026-04']

for date in test_dates:
    params = {
        'get': 'I_COMMODITY,GEN_VAL_MO',
        'COMM_LVL': 'HS6',
        'I_COMMODITY': '854231',
        'time': date,
        'key': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200 and len(response.text) > 10:
        try:
            data = response.json()
            print(f"{date} - {len(data) - 1} rows")
        except Exception:
            print(f"{date} - unparseable response")
    else:
        print(f"{date} - no data (status {response.status_code})")