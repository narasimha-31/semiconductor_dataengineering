import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

DBT_DIR = '/opt/airflow/dbt_project'


@dag(
    dag_id='dag_transform_gold',
    schedule='0 9 * * *',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['transform', 'dbt', 'bigquery']
)
def dag_transform_gold():

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && dbt run --target docker --profiles-dir {DBT_DIR}'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && dbt test --target docker --profiles-dir {DBT_DIR}'
    )

    @task
    def sync_to_bigquery():
        from sync.bq_sync import sync_marts

        db_config = {
            'host': os.getenv('PIPELINE_DB_HOST'),
            'port': int(os.getenv('PIPELINE_DB_PORT')),
            'dbname': os.getenv('PIPELINE_DB_NAME'),
            'user': os.getenv('PIPELINE_DB_USER'),
            'password': os.getenv('PIPELINE_DB_PASSWORD')
        }
        return sync_marts(db_config=db_config)

    dbt_run >> dbt_test >> sync_to_bigquery()


dag_transform_gold()
