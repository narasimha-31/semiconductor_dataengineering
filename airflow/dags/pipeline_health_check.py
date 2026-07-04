import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from confluent_kafka.admin import AdminClient

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def pipeline_db():
    return psycopg2.connect(
        host=os.getenv('PIPELINE_DB_HOST'),
        port=int(os.getenv('PIPELINE_DB_PORT')),
        dbname=os.getenv('PIPELINE_DB_NAME'),
        user=os.getenv('PIPELINE_DB_USER'),
        password=os.getenv('PIPELINE_DB_PASSWORD')
    )


@dag(
    dag_id='pipeline_health_check',
    schedule='@hourly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['monitoring']
)
def pipeline_health_check():

    @task
    def check_postgres():
        conn = pipeline_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM bronze.trade_raw;")
            bronze_trade = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM bronze.regulatory_raw;")
            bronze_reg = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM bronze.filings_raw;")
            bronze_fil = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM silver.trade_cleaned;")
            silver_trade = cursor.fetchone()[0]
        conn.close()
        counts = {'bronze_trade': bronze_trade, 'bronze_regulatory': bronze_reg,
                  'bronze_filings': bronze_fil, 'silver_trade': silver_trade}
        if bronze_trade == 0:
            raise ValueError("Bronze trade is empty - ingestion broken")
        return counts

    @task
    def check_kafka():
        from confluent_kafka import Consumer, TopicPartition

        admin = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')})
        metadata = admin.list_topics(timeout=10)
        topics = [t for t in metadata.topics if not t.startswith('__')]

        probe = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'group.id': 'health-probe'
        })
        low, high = probe.get_watermark_offsets(TopicPartition('semi.dlq', 0),
                                                timeout=10)
        probe.close()
        dlq_depth = high - low
        return {'topic_count': len(topics), 'dlq_kafka_depth': dlq_depth}

    @task
    def check_reconciliation():
        conn = pipeline_db()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT r.hs_code, r.trade_month,
                       ABS(100.0 * (SUM(t.trade_value_usd) - r.api_total)
                           / NULLIF(r.api_total, 0)) AS pct_diff
                FROM silver.reconciliation_totals r
                JOIN silver.trade_cleaned t
                  ON t.hs_code = r.hs_code AND t.trade_month = r.trade_month
                GROUP BY r.hs_code, r.trade_month, r.api_total
                HAVING ABS(100.0 * (SUM(t.trade_value_usd) - r.api_total)
                           / NULLIF(r.api_total, 0)) > 1.0
                ORDER BY 3 DESC
                LIMIT 5;
            """)
            drifted = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM silver.dead_letter_queue;")
            dlq_rows = cursor.fetchone()[0]
        conn.close()
        if drifted:
            raise ValueError(f"Reconciliation drift beyond 1% tolerance: {drifted}")
        return {'drifted_code_months': 0, 'silver_dlq_rows': dlq_rows}

    @task
    def report(db_stats, kafka_stats, recon_stats):
        print(f"PostgreSQL: {db_stats}")
        print(f"Kafka: {kafka_stats}")
        print(f"Reconciliation: {recon_stats}")
        if kafka_stats['dlq_kafka_depth'] > 0:
            print(f"WARNING: {kafka_stats['dlq_kafka_depth']} messages sitting "
                  f"in semi.dlq topic - investigate before retention expires")
        if recon_stats['silver_dlq_rows'] > 0:
            print(f"NOTE: {recon_stats['silver_dlq_rows']} rows in Silver DLQ")

    report(check_postgres(), check_kafka(), check_reconciliation())


pipeline_health_check()