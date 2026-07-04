import requests

BASE_URL = 'https://www.federalregister.gov/api/v1/documents.json'

print("Test 1: how far back does data go?")
params = {
    'conditions[term]': 'semiconductor',
    'order': 'oldest',
    'per_page': 1
}
r = requests.get(BASE_URL, params=params)
oldest = r.json()['results'][0]
print(f"  Oldest match: {oldest['publication_date']} - {oldest['title'][:60]}")

print("\nTest 2: BIS agency filter - volume and range")
params = {
    'conditions[agencies][]': 'industry-and-security-bureau',
    'per_page': 1,
    'order': 'oldest'
}
r = requests.get(BASE_URL, params=params)
data = r.json()
print(f"  Total BIS documents ever: {data['count']}")
print(f"  Oldest: {data['results'][0]['publication_date']}")

print("\nTest 3: the October 2022 export controls - findable?")
params = {
    'conditions[agencies][]': 'industry-and-security-bureau',
    'conditions[publication_date][gte]': '2022-10-01',
    'conditions[publication_date][lte]': '2022-10-31',
    'per_page': 20
}
r = requests.get(BASE_URL, params=params)
data = r.json()
print(f"  BIS docs published Oct 2022: {data['count']}")
for doc in data['results']:
    print(f"    {doc['publication_date']} [{doc['type']}] {doc['title'][:70]}")