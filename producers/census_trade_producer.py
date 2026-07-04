import os
import json
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from jsonschema import validate, ValidationError

load_dotenv()

API_KEY = os.getenv('CENSUS_API_KEY')
BASE_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'
TOPIC = 'semi.trade.raw'
DLQ_TOPIC = 'semi.dlq'
HS_CODES = ['854231', '854232', '854233', '854239', '854290']
START_MONTH = '2010-01'
END_MONTH = '2026-04'

MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "cty_code": {"type": "string", "minLength": 1},
        "cty_name": {"type": "string", "minLength": 1},
        "hs_code": {"type": "string", "pattern": "^854[12][0-9]{2}$"},
        "trade_value_usd": {"type": "string", "pattern": "^[0-9]+$"},
        "month": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}$"}
    },
    "required": ["cty_code", "cty_name", "hs_code", "trade_value_usd", "month"]
}

stats = {"sent": 0, "dlq": 0, "delivered": 0, "failed": 0}


def delivery_report(err, msg):
    if err is not None:
        stats["failed"] += 1
        print(f"Delivery failed for key {msg.key()}: {err}")
    else:
        stats["delivered"] += 1


def fetch_trade_data(hs_code, start_month, end_month):
    params = {
        'get': 'CTY_CODE,CTY_NAME,GEN_VAL_MO',
        'COMM_LVL': 'HS6',
        'I_COMMODITY': hs_code,
        'time': f'from {start_month} to {end_month}',
        'key': API_KEY
    }
    response = requests.get(BASE_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def row_to_message(header, row):
    record = dict(zip(header, row))
    return {
        "cty_code": record.get("CTY_CODE", ""),
        "cty_name": record.get("CTY_NAME", ""),
        "hs_code": record.get("I_COMMODITY", ""),
        "trade_value_usd": record.get("GEN_VAL_MO", ""),
        "month": record.get("time", "")
    }


def run_ingestion(start_month, end_month, bootstrap_servers='localhost:9092'):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    stats = {"sent": 0, "dlq": 0, "delivered": 0, "failed": 0}

    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed for key {msg.key()}: {err}")
        else:
            stats["delivered"] += 1

    for hs_code in HS_CODES:
        print(f"Fetching {hs_code} from {start_month} to {end_month}...")
        data = fetch_trade_data(hs_code, start_month, end_month)
        header = data[0]
        rows = data[1:]
        print(f"  {len(rows)} rows received")

        for row in rows:
            message = row_to_message(header, row)
            key = f"{message['cty_code']}-{message['hs_code']}"
            try:
                validate(instance=message, schema=MESSAGE_SCHEMA)
                producer.produce(topic=TOPIC, key=key,
                                 value=json.dumps(message),
                                 callback=delivery_report)
                stats["sent"] += 1
            except ValidationError as e:
                dlq_message = {
                    "original": message,
                    "error_reason": e.message,
                    "source_topic": TOPIC,
                    "hs_code_batch": hs_code
                }
                producer.produce(topic=DLQ_TOPIC, key=key,
                                 value=json.dumps(dlq_message))
                stats["dlq"] += 1
            producer.poll(0)

    producer.flush()
    print(f"Done. Sent: {stats['sent']}, DLQ: {stats['dlq']}, "
          f"Delivered: {stats['delivered']}, Failed: {stats['failed']}")
    return stats


if __name__ == "__main__":
    run_ingestion(START_MONTH, END_MONTH)