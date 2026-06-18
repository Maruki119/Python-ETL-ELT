from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


INPUT_FILE = "/etc/passwd"

DATA_DIR = Path("/tmp/airflow_etl")
EXTRACTED_FILE = DATA_DIR / "extracted-data.txt"
TRANSFORMED_FILE = DATA_DIR / "transformed.txt"
OUTPUT_FILE = DATA_DIR / "data_for_analytics.csv"


def extract():
    print("Inside Extract")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(INPUT_FILE, "r") as infile, open(EXTRACTED_FILE, "w") as outfile:
        for line in infile:
            fields = line.strip().split(":")

            if len(fields) >= 6:
                username = fields[0]
                user_id = fields[2]
                home_directory = fields[5]

                outfile.write(f"{username}:{user_id}:{home_directory}\n")


def transform():
    print("Inside Transform")

    with open(EXTRACTED_FILE, "r") as infile, open(TRANSFORMED_FILE, "w") as outfile:
        for line in infile:
            processed_line = line.strip().replace(":", ",")
            outfile.write(processed_line + "\n")


def load():
    print("Inside Load")

    with open(TRANSFORMED_FILE, "r") as infile, open(OUTPUT_FILE, "w") as outfile:
        outfile.write("username,user_id,home_directory\n")

        for line in infile:
            outfile.write(line.strip() + "\n")


def check():
    print("Inside Check")

    with open(OUTPUT_FILE, "r") as infile:
        for line in infile:
            print(line.strip())


default_args = {
    "owner": "Nititorn Kitprasopchok",
    "email": ["your-email@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="my-first-python-etl-dag",
    default_args=default_args,
    description="My first Python ETL DAG",
    start_date=pendulum.datetime(2026, 6, 18, tz="UTC"),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["etl", "python"],
) as dag:

    execute_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    execute_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    execute_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    execute_check = PythonOperator(
        task_id="check",
        python_callable=check,
    )

    execute_extract >> execute_transform >> execute_load >> execute_check