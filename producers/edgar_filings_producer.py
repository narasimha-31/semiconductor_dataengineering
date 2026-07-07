import os
import json
import time
import requests
from datetime import date
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
        "period_type": {"type": "string", "enum": ["Q", "FY", "INSTANT"]},
        "fiscal_year": {"type": ["integer", "null"]},
        "fiscal_period": {"type": ["string", "null"]},
        "filed": {"type": ["string", "null"]}
    },
    "required": ["ticker", "cik", "metric", "tag", "end_date", "value",
                 "form", "period_type"]
}


def classify_period(start, end):
    try:
        s = date.fromisoformat(start)
        e = date.fromisoformat(end)
    except (TypeError, ValueError):
        return None
    days = (e - s).days
    if 75 <= days <= 105:
        return 'Q'
    if 350 <= days <= 380:
        return 'FY'
    return 'OTHER'


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
            if metric == 'inventory':
                period_type = 'INSTANT'
            else:
                period_type = classify_period(p.get('start'), end)
                if period_type not in ('Q', 'FY'):
                    continue
            p['_tag'] = tag
            p['_period_type'] = period_type
            dedupe_key = (end, period_type)
            filed = p.get('filed') or ''
            if dedupe_key not in best or filed > (best[dedupe_key].get('filed') or ''):
                best[dedupe_key] = p
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
                    "period_type": p.get('_period_type'),
                    "fiscal_year": p.get('fy'),
                    "fiscal_period": p.get('fp'),
                    "filed": p.get('filed')
                }
                key = f"{ticker}-{metric}-{p['end']}-{p.get('_period_type')}"
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