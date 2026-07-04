import requests
import json

BASE_URL = 'https://www.federalregister.gov/api/v1/documents.json'

params = {
    'conditions[term]': 'semiconductor',
    'per_page': 3
}

response = requests.get(BASE_URL, params=params)
print(f"Status: {response.status_code}")

data = response.json()
print(f"Type: {type(data)}")

if isinstance(data, dict):
    print(f"Top-level keys: {list(data.keys())}")
    if 'results' in data:
        print(f"Total matching documents: {data.get('count')}")
        print(f"Records in this page: {len(data['results'])}")
        first = data['results'][0]
        print(f"\nFields in one record:")
        for key, value in first.items():
            preview = str(value)[:80]
            print(f"  {key}: {preview}")