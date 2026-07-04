import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from confluent_kafka.admin import AdminClient

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='pipeline_health_check',
    schedule='@hourly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=['monitoring']
)
def pipeline_health_check():

    @task
    def check_postgres():
        conn = psycopg2.connect(
            host=os.getenv('PIPELINE_DB_HOST'),
            port=int(os.getenv('PIPELINE_DB_PORT')),
            dbname=os.getenv('PIPELINE_DB_NAME'),
            user=os.getenv('PIPELINE_DB_USER'),
            password=os.getenv('PIPELINE_DB_PASSWORD')
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM bronze.trade_raw;")
            bronze_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM silver.trade_cleaned;")
            silver_count = cursor.fetchone()[0]
        conn.close()
        return {'bronze_rows': bronze_count, 'silver_rows': silver_count}

    @task
    def check_kafka():
        admin = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')})
        metadata = admin.list_topics(timeout=10)
        topics = [t for t in metadata.topics if not t.startswith('__')]
        return {'topic_count': len(topics), 'topics': sorted(topics)}

    @task
    def report(db_stats, kafka_stats):
        print(f"PostgreSQL - bronze: {db_stats['bronze_rows']}, silver: {db_stats['silver_rows']}")
        print(f"Kafka - {kafka_stats['topic_count']} topics: {kafka_stats['topics']}")
        if db_stats['bronze_rows'] == 0:
            raise ValueError("Bronze is empty - upstream ingestion may be broken")


    report(check_postgres(), check_kafka())


pipeline_health_check()