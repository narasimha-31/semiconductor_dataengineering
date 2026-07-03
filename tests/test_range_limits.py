import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CENSUS_API_KEY')
BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

def fetch_range(commodity, start, end):
    params = {
        'get': 'CTY_CODE,CTY_NAME,GEN_VAL_MO',
        'COMM_LVL': 'HS6',
        'I_COMMODITY': commodity,
        'time': f'from {start} to {end}',
        'key': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200 and len(response.text) > 10:
        data = response.json()
        months = set()
        for row in data[1:]:
            months.add(row[-1])
        return len(data) - 1, len(months)
    return None, response.status_code

print("Full 16-year range, one code:")
rows, months = fetch_range('854231', '2010-01', '2026-04')
print(f"  rows: {rows}, distinct months: {months}")

print("One year, same code (for comparison):")
rows_y, months_y = fetch_range('854231', '2024-01', '2024-12')
print(f"  rows: {rows_y}, distinct months: {months_y}")