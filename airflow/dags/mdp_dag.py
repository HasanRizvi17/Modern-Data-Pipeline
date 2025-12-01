from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.dates import datetime, timedelta

DBT_DIR = "/opt/airflow/dbt_project"
DBT_TARGET = "dev"

default_args = {
    "owner": "airflow",
    'email_on_failure': True,
    'email': ['hasanrizvi.170@gmail.com']
}

with DAG(
    dag_id="dbt_pipeline", # the DAG's name we see in the Airflow UI
    description="Run dbt models and tests daily",
    start_date=datetime(2025, 11, 25),
    schedule_interval="15 14 * * *",   # runs every day at 2:15pm
    catchup=False,
    default_args=default_args
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=1),
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --select mdp_stg --target {DBT_TARGET}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --select mdp_stg --target {DBT_TARGET}"
    )

    dbt_run >> dbt_test
