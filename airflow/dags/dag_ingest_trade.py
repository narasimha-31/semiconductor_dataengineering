import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='dag_ingest_trade',
    schedule='0 6 15 * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=['ingestion', 'census']
)
def dag_ingest_trade():

    @task
    def ingest_latest_month(logical_date=None):
        from producers.census_trade_producer import run_ingestion

        target = logical_date - timedelta(days=75)
        target_month = target.strftime('%Y-%m')

        print(f"Run date {logical_date.date()}, targeting month {target_month}")
        stats = run_ingestion(
            start_month=target_month,
            end_month=target_month,
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        )

        if stats['sent'] == 0 and stats['dlq'] == 0:
            raise ValueError(f"No rows returned for {target_month} - "
                             "data may not be published yet")
        if stats['failed'] > 0:
            raise ValueError(f"{stats['failed']} Kafka deliveries failed")
        return stats

    ingest_latest_month()


dag_ingest_trade()