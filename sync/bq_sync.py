import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

MARTS = ['mart_hhi_concentration', 'mart_regulatory_impact', 'mart_company_signals']


def get_pg_connection(db_config=None):
    if db_config is None:
        db_config = {
            'host': 'localhost',
            'port': int(os.getenv('POSTGRES_PORT')),
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
    return psycopg2.connect(**db_config)


def sync_marts(db_config=None, project_id=None, dataset='semi_gold'):
    project_id = project_id or os.getenv('GCP_PROJECT_ID')
    bq = bigquery.Client(project=project_id)
    conn = get_pg_connection(db_config)
    stats = {}

    for mart in MARTS:
        df = pd.read_sql(f"SELECT * FROM gold.{mart}", conn)
        table_id = f"{project_id}.{dataset}.{mart}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        table = bq.get_table(table_id)
        stats[mart] = {'pg_rows': len(df), 'bq_rows': table.num_rows}
        print(f"{mart}: {len(df)} rows -> BigQuery ({table.num_rows} confirmed)")
        if len(df) != table.num_rows:
            raise ValueError(f"{mart}: row count mismatch after sync")

    conn.close()
    return stats


if __name__ == "__main__":
    sync_marts()
