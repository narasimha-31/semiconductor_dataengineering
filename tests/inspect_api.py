import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('CENSUS_API_KEY')

BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

# Minimal request - just a few columns, one HS code, one month
params = {
    'get': 'CTY_CODE,CTY_NAME,I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,GEN_QY1_MO',
    'COMM_LVL': 'HS6',
    'I_COMMODITY': '854231',
    'time': '2024-01',
    'key': API_KEY
}

response = requests.get(BASE_URL, params=params)
print(f"Status code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"Type of data: {type(data)}")
    print(f"Number of rows: {len(data)}")
    print(f"Type of first row: {type(data[0])}")
    print(f"\n--- First 10 rows ---")
    for row in data[:10]:
        print(row)
else:
    print(f"Error: {response.text}")