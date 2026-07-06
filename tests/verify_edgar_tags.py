import requests
import time

HEADERS = {'User-Agent': 'Narasimha Pola narasimharoyal31@gmail.com'}

CIKS = {
    'INTC': '0000050863', 'NVDA': '0001045810', 'AMD': '0000002488',
    'MU': '0000723125', 'TXN': '0000097476', 'QCOM': '0000804328',
    'AVGO': '0001730168', 'GFS': '0001709048', 'TSM': '0001046179',
    'ON': '0001097864'
}

CANDIDATES = {
    'revenue': {
        'us-gaap': ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet'],
        'ifrs-full': ['Revenue']
    },
    'capex': {
        'us-gaap': ['PaymentsToAcquirePropertyPlantAndEquipment'],
        'ifrs-full': ['PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivitiesContinuingOperations', 'PurchaseOfPropertyPlantAndEquipment']
    },
    'inventory': {
        'us-gaap': ['InventoryNet'],
        'ifrs-full': ['Inventories']
    }
}

for ticker, cik in CIKS.items():
    r = requests.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
                     headers=HEADERS, timeout=60)
    facts = r.json()['facts']
    taxonomy = 'us-gaap' if 'us-gaap' in facts else 'ifrs-full'
    line = f"{ticker} [{taxonomy}]"
    for metric, tax_map in CANDIDATES.items():
        found = None
        for tag in tax_map.get(taxonomy, []):
            if tag in facts.get(taxonomy, {}):
                units = facts[taxonomy][tag].get('units', {})
                points = sum(len(v) for v in units.values())
                found = f"{metric}: {tag[:40]} ({points} pts)"
                break
        line += f"\n    {found or metric + ': MISSING'}"
    print(line)
    time.sleep(0.3)
