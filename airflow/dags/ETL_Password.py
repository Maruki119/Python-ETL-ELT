from datetime import timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator

DATA_DIR = "/opt/airflow/data"

default_args = {
    "owner": "Nititorn Kitprasopchok",
    "start_date": pendulum.datetime(2026, 6, 18, tz="UTC"),
    "email": ["nititorn.kij@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_password",
    default_args=default_args,
    description="etl_password",
    schedule=timedelta(days=1),
    catchup=False,
)

extract = BashOperator(
    task_id="extract",
    bash_command=f"""
    mkdir -p {DATA_DIR}
    cut -d":" -f1,3,6 /etc/passwd > {DATA_DIR}/extracted-data.txt
    ls -l {DATA_DIR}
    """,
    dag=dag,
)

transform_and_load = BashOperator(
    task_id="transform",
    bash_command=f"""
    ls -l {DATA_DIR}
    tr ":" "," < {DATA_DIR}/extracted-data.txt > {DATA_DIR}/transformed-data.csv
    ls -l {DATA_DIR}
    """,
    dag=dag,
)

extract >> transform_and_load