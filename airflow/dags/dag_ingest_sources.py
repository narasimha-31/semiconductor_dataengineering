import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='dag_ingest_regulatory',
    schedule='0 7 * * *',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['ingestion', 'federal-register']
)
def dag_ingest_regulatory():

    @task
    def ingest_recent_docs(logical_date=None):
        from producers.federal_register_producer import run_ingestion

        start = (logical_date - timedelta(days=7)).strftime('%Y-%m-%d')
        end = logical_date.strftime('%Y-%m-%d')
        print(f"Fetching BIS documents {start} to {end}")
        stats = run_ingestion(
            start_date=start,
            end_date=end,
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        )
        if stats['failed'] > 0:
            raise ValueError(f"{stats['failed']} Kafka deliveries failed")
        return stats

    @task
    def drain_regulatory(producer_stats):
        from consumers.regulatory_consumer import run_consumer

        db_config = {
            'host': os.getenv('PIPELINE_DB_HOST'),
            'port': int(os.getenv('PIPELINE_DB_PORT')),
            'dbname': os.getenv('PIPELINE_DB_NAME'),
            'user': os.getenv('PIPELINE_DB_USER'),
            'password': os.getenv('PIPELINE_DB_PASSWORD')
        }
        return run_consumer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            db_config=db_config,
            batch_prefix='daily'
        )

    drain_regulatory(ingest_recent_docs())


@dag(
    dag_id='dag_ingest_filings',
    schedule='0 8 * * 1',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['ingestion', 'edgar']
)
def dag_ingest_filings():

    @task
    def ingest_filings():
        from producers.edgar_filings_producer import run_ingestion

        stats = run_ingestion(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        )
        if stats['failed'] > 0:
            raise ValueError(f"{stats['failed']} Kafka deliveries failed")
        return stats

    @task
    def drain_filings(producer_stats):
        from consumers.filings_consumer import run_consumer

        db_config = {
            'host': os.getenv('PIPELINE_DB_HOST'),
            'port': int(os.getenv('PIPELINE_DB_PORT')),
            'dbname': os.getenv('PIPELINE_DB_NAME'),
            'user': os.getenv('PIPELINE_DB_USER'),
            'password': os.getenv('PIPELINE_DB_PASSWORD')
        }
        return run_consumer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            db_config=db_config,
            batch_prefix='weekly'
        )

    drain_filings(ingest_filings())


dag_ingest_regulatory()
dag_ingest_filings()