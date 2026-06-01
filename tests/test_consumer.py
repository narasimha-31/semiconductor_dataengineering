from confluent_kafka import Consumer
import json

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['test.hello'])

print("Waiting for messages... (press Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        key = msg.key().decode('utf-8')
        value = json.loads(msg.value().decode('utf-8'))

        print(f"Key: {key}")
        print(f"Value: {value}")
        print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
        print("---")

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    consumer.close()