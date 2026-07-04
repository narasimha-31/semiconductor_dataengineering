import os
import json
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from jsonschema import validate, ValidationError

load_dotenv()

BASE_URL = 'https://www.federalregister.gov/api/v1/documents.json'
TOPIC = 'semi.regulatory.raw'
DLQ_TOPIC = 'semi.dlq'
BIS_SLUG = 'industry-and-security-bureau'

MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "document_number": {"type": "string", "minLength": 1},
        "title": {"type": "string", "minLength": 1},
        "doc_type": {"type": "string", "minLength": 1},
        "abstract": {"type": ["string", "null"]},
        "publication_date": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
        "html_url": {"type": ["string", "null"]},
        "agencies": {"type": "array"}
    },
    "required": ["document_number", "title", "doc_type", "publication_date"]
}


def fetch_documents(start_date=None, end_date=None):
    params = {
        'conditions[agencies][]': BIS_SLUG,
        'per_page': 1000,
        'order': 'oldest',
        'fields[]': ['document_number', 'title', 'type', 'abstract',
                     'publication_date', 'html_url', 'agencies']
    }
    if start_date:
        params['conditions[publication_date][gte]'] = start_date
    if end_date:
        params['conditions[publication_date][lte]'] = end_date

    documents = []
    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    documents.extend(data.get('results', []))
    while data.get('next_page_url'):
        response = requests.get(data['next_page_url'], timeout=60)
        response.raise_for_status()
        data = response.json()
        documents.extend(data.get('results', []))
    return documents


def record_to_message(record):
    agencies = record.get('agencies') or []
    agency_names = [a.get('name') for a in agencies
                    if isinstance(a, dict) and a.get('name')]
    return {
        "document_number": record.get('document_number', ''),
        "title": record.get('title', ''),
        "doc_type": record.get('type', ''),
        "abstract": record.get('abstract'),
        "publication_date": record.get('publication_date', ''),
        "html_url": record.get('html_url'),
        "agencies": agency_names
    }


def run_ingestion(start_date=None, end_date=None,
                  bootstrap_servers='localhost:9092'):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    stats = {"sent": 0, "dlq": 0, "delivered": 0, "failed": 0}

    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed for key {msg.key()}: {err}")
        else:
            stats["delivered"] += 1

    print(f"Fetching BIS documents ({start_date or 'beginning'} to {end_date or 'today'})...")
    documents = fetch_documents(start_date, end_date)
    print(f"  {len(documents)} documents received")

    for record in documents:
        message = record_to_message(record)
        key = message['document_number'] or 'unknown'
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
                "source_topic": TOPIC
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
    run_ingestion()