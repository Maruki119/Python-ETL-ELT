from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "Nititorn Kitprasopchok",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="greeting_dag",
    description="A simple DAG that prints a greeting message",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=1),
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="print_greeting",
        bash_command='echo "Hello, Airflow!"',
    )

    task2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task1 >> task2