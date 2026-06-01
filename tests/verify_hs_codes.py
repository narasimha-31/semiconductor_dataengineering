import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CENSUS_API_KEY')

BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

codes_to_check = ['854231', '854232', '854233', '854239', '854290',
                  '854110', '854120', '854130', '854140', '854150',
                  '854160', '854170', '854190']

for code in codes_to_check:
    params = {
        'get': 'I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO',
        'COMM_LVL': 'HS6',
        'I_COMMODITY': code,
        'time': '2024-01',
        'key': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200 and len(response.text) > 10:
        try:
            data = response.json()
            if len(data) > 1:
                # Count rows and sum the total value across all countries
                row_count = len(data) - 1
                total_value = 0
                for row in data[1:]:
                    try:
                        total_value += int(row[2])
                    except:
                        pass
                print(f"  {data[1][0]} - {data[1][1]}")
                print(f"    Countries: {row_count}, Total import value: ${total_value:,}")
        except:
            print(f"  {code} - response not parseable")
    else:
        print(f"  {code} - no data")