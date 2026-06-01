from confluent_kafka import Producer
import json

config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(config)

message = {
    'country': 'Taiwan',
    'hs_code': '8542',
    'trade_value_usd': 5200000000,
    'month': '2026-03'
}

producer.produce(
    topic='test.hello',
    key='TW',
    value=json.dumps(message)
)

producer.flush()

print("Message sent successfully!")