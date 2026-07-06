import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='dag_ingest_trade',
    schedule='0 6 15 * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
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
            raise ValueError(f"No rows returned for {target_month}")
        if stats['failed'] > 0:
            raise ValueError(f"{stats['failed']} Kafka deliveries failed")
        return {'target_month': target_month, 'delivered': stats['delivered']}

    @task
    def drain_to_bronze(producer_result):
        import psycopg2
        from consumers.census_trade_consumer import run_consumer

        db_config = {
            'host': os.getenv('PIPELINE_DB_HOST'),
            'port': int(os.getenv('PIPELINE_DB_PORT')),
            'dbname': os.getenv('PIPELINE_DB_NAME'),
            'user': os.getenv('PIPELINE_DB_USER'),
            'password': os.getenv('PIPELINE_DB_PASSWORD')
        }
        run_consumer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            db_config=db_config,
            batch_prefix='monthly'
        )

        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM bronze.trade_raw WHERE month = %s",
                (producer_result['target_month'],)
            )
            bronze_count = cursor.fetchone()[0]
        conn.close()

        expected = producer_result['delivered']
        print(f"Bronze holds {bronze_count} rows for "
              f"{producer_result['target_month']}, producer fetched {expected}")
        if bronze_count < expected:
            raise ValueError(f"Bronze has {bronze_count} rows for month "
                             f"{producer_result['target_month']} but producer "
                             f"delivered {expected} - data missing")
        return {'bronze_rows_for_month': bronze_count}

    transform_silver = SQLExecuteQueryOperator(
        task_id='transform_silver',
        conn_id='pipeline_postgres',
        sql='sql/transforms/bronze_to_silver_trade.sql'
    )

    

    @task
    def ge_checkpoint():
        from quality.ge_trade_suite import run_trade_checkpoint

        db_config = {
            'host': os.getenv('PIPELINE_DB_HOST'),
            'port': os.getenv('PIPELINE_DB_PORT'),
            'dbname': os.getenv('PIPELINE_DB_NAME'),
            'user': os.getenv('PIPELINE_DB_USER'),
            'password': os.getenv('PIPELINE_DB_PASSWORD')
        }
        stats = run_trade_checkpoint(db_config)
        if not stats['success']:
            raise ValueError(f"GE gate failed: {stats['failed']} expectations violated")
        return stats

    drain_to_bronze(ingest_latest_month()) >> ge_checkpoint() >> transform_silver


dag_ingest_trade()