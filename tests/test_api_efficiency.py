import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CENSUS_API_KEY')
BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

print("Test 1: comma-separated HS codes in one call")
params1 = {
    'get': 'CTY_CODE,CTY_NAME,GEN_VAL_MO',
    'COMM_LVL': 'HS6',
    'I_COMMODITY': '854231,854232',
    'time': '2024-01',
    'key': API_KEY
}
r1 = requests.get(BASE_URL, params=params1)
print(f"status: {r1.status_code}")
print(f"first 300 chars: {r1.text[:300]}\n")

print("Test 2: time range syntax")
params2 = {
    'get': 'CTY_CODE,CTY_NAME,GEN_VAL_MO',
    'COMM_LVL': 'HS6',
    'I_COMMODITY': '854231',
    'time': 'from 2024-01 to 2024-03',
    'key': API_KEY
}
r2 = requests.get(BASE_URL, params=params2)
print(f"status: {r2.status_code}")
if r2.status_code == 200 and len(r2.text) > 10:
    data = r2.json()
    months = set()
    for row in data[1:]:
        months.add(row[-1])
    print(f"rows: {len(data) - 1}, distinct months returned: {sorted(months)}")
else:
    print(f"first 300 chars: {r2.text[:300]}")