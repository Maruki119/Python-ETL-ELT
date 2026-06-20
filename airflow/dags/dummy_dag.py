# import the libraries

from datetime import timedelta
import pendulum
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.providers.standard.operators.bash import BashOperator

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Nititorn Kitprasopchok',
    'start_date': pendulum.datetime(2026, 1, 1, tz="UTC"),
    'email': ['nititorn.kij@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='My first DAG',
    schedule=timedelta(minutes=1),
)

# define the tasks

# define the first task

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 1',
    dag=dag,
)

# define the second task
task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 2',
    dag=dag,
)

# define the third task
task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 3',
    dag=dag,
)

# task pipeline
task1 >> task2 >> task3
