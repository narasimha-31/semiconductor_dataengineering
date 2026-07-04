import os
import json
import time
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from jsonschema import validate, ValidationError

load_dotenv()

TOPIC = 'semi.filings.raw'
DLQ_TOPIC = 'semi.dlq'

CIKS = {
    'INTC': '0000050863', 'NVDA': '0001045810', 'AMD': '0000002488',
    'MU': '0000723125', 'TXN': '0000097476', 'QCOM': '0000804328',
    'AVGO': '0001730168', 'GFS': '0001709048', 'TSM': '0001046179',
    'ON': '0001097864'
}

CANDIDATES = {
    'revenue': {
        'us-gaap': ['RevenueFromContractWithCustomerExcludingAssessedTax',
                    'Revenues', 'SalesRevenueNet'],
        'ifrs-full': ['Revenue']
    },
    'capex': {
        'us-gaap': ['PaymentsToAcquirePropertyPlantAndEquipment',
                    'PaymentsToAcquireProductiveAssets'],
        'ifrs-full': ['PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivitiesContinuingOperations',
                      'PurchaseOfPropertyPlantAndEquipment',
                      'PaymentsForPropertyPlantAndEquipmentClassifiedAsInvestingActivities']
    },
    'inventory': {
        'us-gaap': ['InventoryNet'],
        'ifrs-full': ['Inventories']
    }
}

VALID_FORMS = {'10-K', '10-Q', '20-F', '6-K'}

MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string", "minLength": 1},
        "cik": {"type": "string", "pattern": "^[0-9]{10}$"},
        "taxonomy": {"type": "string"},
        "metric": {"type": "string", "enum": ["revenue", "capex", "inventory"]},
        "tag": {"type": "string", "minLength": 1},
        "end_date": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
        "value": {"type": "number"},
        "form": {"type": "string"},
        "fiscal_year": {"type": ["integer", "null"]},
        "fiscal_period": {"type": ["string", "null"]},
        "filed": {"type": ["string", "null"]}
    },
    "required": ["ticker", "cik", "metric", "tag", "end_date", "value", "form"]
}


def fetch_company_facts(cik, user_agent):
    url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
    response = requests.get(url, headers={'User-Agent': user_agent}, timeout=60)
    response.raise_for_status()
    return response.json()['facts']


def extract_metric_points(facts, taxonomy, metric):
    best = {}
    tags_used = []
    for tag in CANDIDATES[metric].get(taxonomy, []):
        if tag not in facts.get(taxonomy, {}):
            continue
        tags_used.append(tag)
        units = facts[taxonomy][tag].get('units', {})
        for p in units.get('USD', []):
            if p.get('form') not in VALID_FORMS:
                continue
            end = p.get('end')
            if not end or p.get('val') is None:
                continue
            p['_tag'] = tag
            filed = p.get('filed') or ''
            if end not in best or filed > (best[end].get('filed') or ''):
                best[end] = p
    if not tags_used:
        return None, []
    return tags_used[0], list(best.values())


def run_ingestion(bootstrap_servers='localhost:9092'):
    user_agent = os.getenv('SEC_USER_AGENT')
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    stats = {"sent": 0, "dlq": 0, "delivered": 0, "failed": 0, "missing": []}

    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed for key {msg.key()}: {err}")
        else:
            stats["delivered"] += 1

    for ticker, cik in CIKS.items():
        print(f"Fetching {ticker}...")
        facts = fetch_company_facts(cik, user_agent)
        taxonomy = 'us-gaap' if 'us-gaap' in facts else 'ifrs-full'

        for metric in CANDIDATES:
            tag, points = extract_metric_points(facts, taxonomy, metric)
            if tag is None:
                stats["missing"].append(f"{ticker}:{metric}")
                continue
            for p in points:
                message = {
                    "ticker": ticker,
                    "cik": cik,
                    "taxonomy": taxonomy,
                    "metric": metric,
                    "tag": p.get('_tag', tag),
                    "end_date": p['end'],
                    "value": p['val'],
                    "form": p['form'],
                    "fiscal_year": p.get('fy'),
                    "fiscal_period": p.get('fp'),
                    "filed": p.get('filed')
                }
                key = f"{ticker}-{metric}-{p['end']}"
                try:
                    validate(instance=message, schema=MESSAGE_SCHEMA)
                    producer.produce(topic=TOPIC, key=key,
                                     value=json.dumps(message),
                                     callback=delivery_report)
                    stats["sent"] += 1
                except ValidationError as e:
                    producer.produce(topic=DLQ_TOPIC, key=key,
                                     value=json.dumps({"original": message,
                                                       "error_reason": e.message,
                                                       "source_topic": TOPIC}))
                    stats["dlq"] += 1
                producer.poll(0)
        time.sleep(0.3)

    producer.flush()
    print(f"Done. Sent: {stats['sent']}, DLQ: {stats['dlq']}, "
          f"Delivered: {stats['delivered']}, Failed: {stats['failed']}")
    if stats["missing"]:
        print(f"Coverage gaps (stored as absent, documented): {stats['missing']}")
    return stats


if __name__ == "__main__":
    run_ingestion()