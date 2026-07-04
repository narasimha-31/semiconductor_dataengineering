import os
import json
import uuid
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

TOPIC = 'semi.filings.raw'
BATCH_SIZE = 500
IDLE_TIMEOUT_SECONDS = 30

INSERT_SQL = """
    INSERT INTO bronze.filings_raw
        (ticker, cik, taxonomy, metric, tag, end_date, value, form,
         fiscal_year, fiscal_period, filed,
         kafka_partition, kafka_offset, batch_id)
    VALUES %s
    ON CONFLICT ON CONSTRAINT uq_filings_raw DO NOTHING
"""


def get_connection(db_config=None):
    if db_config is None:
        db_config = {
            'host': 'localhost',
            'port': int(os.getenv('POSTGRES_PORT')),
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
    return psycopg2.connect(**db_config)


def insert_batch(conn, consumer, batch, batch_id, stats):
    rows = []
    for msg in batch:
        record = json.loads(msg.value().decode('utf-8'))
        rows.append((
            record['ticker'], record['cik'], record['taxonomy'],
            record['metric'], record['tag'], record['end_date'],
            record['value'], record['form'], record['fiscal_year'],
            record['fiscal_period'], record['filed'],
            msg.partition(), msg.offset(), batch_id
        ))
    with conn.cursor() as cursor:
        execute_values(cursor, INSERT_SQL, rows, page_size=len(rows))
        inserted = cursor.rowcount
    conn.commit()
    consumer.commit(asynchronous=False)
    stats['consumed'] += len(rows)
    stats['inserted'] += inserted
    stats['duplicates'] += len(rows) - inserted


def run_consumer(bootstrap_servers='localhost:9092', db_config=None,
                 batch_prefix='backfill'):
    batch_id = f"{batch_prefix}-{uuid.uuid4().hex[:8]}"
    stats = {'consumed': 0, 'inserted': 0, 'duplicates': 0}

    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'bronze-filings-writer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([TOPIC])
    conn = get_connection(db_config)
    batch = []
    idle_seconds = 0

    print(f"Consuming from {TOPIC} as batch {batch_id}...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                idle_seconds += 1
                if batch:
                    insert_batch(conn, consumer, batch, batch_id, stats)
                    batch = []
                if idle_seconds >= IDLE_TIMEOUT_SECONDS:
                    print("No new messages, stopping.")
                    break
                continue
            idle_seconds = 0
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            batch.append(msg)
            if len(batch) >= BATCH_SIZE:
                insert_batch(conn, consumer, batch, batch_id, stats)
                batch = []
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        if batch:
            insert_batch(conn, consumer, batch, batch_id, stats)
        consumer.close()
        conn.close()

    print(f"Done. Consumed: {stats['consumed']}, Inserted: {stats['inserted']}, "
          f"Duplicates skipped: {stats['duplicates']}")
    return stats


if __name__ == "__main__":
    run_consumer()