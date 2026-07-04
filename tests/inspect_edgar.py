import requests

import os
from dotenv import load_dotenv

load_dotenv()

HEADERS = {'User-Agent': os.getenv('SEC_USER_AGENT')}

print("Step 1: resolve tickers to CIK numbers")
r = requests.get('https://www.sec.gov/files/company_tickers.json',
                 headers=HEADERS, timeout=60)
companies = r.json()

targets = ['INTC', 'NVDA', 'AMD', 'MU', 'TXN', 'QCOM', 'AVGO', 'GFS', 'TSM', 'ON']
cik_map = {}
for entry in companies.values():
    if entry['ticker'] in targets:
        cik_map[entry['ticker']] = str(entry['cik_str']).zfill(10)

for ticker in targets:
    print(f"  {ticker}: {cik_map.get(ticker, 'NOT FOUND')}")

print("\nStep 2: what financial tags does Intel report?")
cik = cik_map['INTC']
r = requests.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
                 headers=HEADERS, timeout=60)
facts = r.json()
print(f"  Taxonomies: {list(facts['facts'].keys())}")
gaap_tags = list(facts['facts'].get('us-gaap', {}).keys())
print(f"  Total us-gaap tags: {len(gaap_tags)}")
for keyword in ['Revenue', 'CapitalExpenditure', 'Inventory']:
    matches = [t for t in gaap_tags if keyword.lower() in t.lower()]
    print(f"  Tags containing '{keyword}': {matches[:5]}")

print("\nStep 3: same check for TSMC (foreign filer)")
cik = cik_map['TSM']
r = requests.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
                 headers=HEADERS, timeout=60)
facts = r.json()
print(f"  Taxonomies: {list(facts['facts'].keys())}")