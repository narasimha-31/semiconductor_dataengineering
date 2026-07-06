from datetime import datetime, timedelta

from airflow.decorators import dag
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
    tags=['transform', 'dbt']
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

    dbt_run >> dbt_test


dag_transform_gold()