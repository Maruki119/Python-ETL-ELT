import random
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Nititorn Kitprasopchok',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def choose_branch():
    """Return the task_id for the chosen branch."""
    return 'branch_b' if random.choice([True, False]) else 'branch_c'

with DAG(
    dag_id='example_branch_operator',
    default_args=default_args,
    description='A simple Airflow DAG demonstrating BranchPythonOperator',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting the DAG"',
    )

    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
    )

    branch_b = BashOperator(
        task_id='branch_b',
        bash_command='echo "Branch B was selected"',
    )

    branch_c = BashOperator(
        task_id='branch_c',
        bash_command='echo "Branch C was selected"',
    )

    join = EmptyOperator(
        task_id='join',
        trigger_rule='none_failed_min_one_success',
    )

    start >> branch >> [branch_b, branch_c] >> join
