from datetime import datetime, timedelta
from pathlib import Path
import requests
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
input_file = "web-server-access-log.txt"
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
output_file = 'capitalized.txt'

def download_file():
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful
    with open(input_file, "wb") as f:
        f.write(response.content)
    print(f"File downloaded successfully: {input_file}")
        
def extract():
    global input_file
    print("Inside Extract")
    with open(input_file, "r") as infile:
        with open(extracted_file, "w") as outfile:
            for line in infile:
                fields = line.split('#')
                if len(fields) >= 4:
                    field_1 = fields[0]
                    field_4 = fields[3]
                    outfile.write(field_1 + "#" + field_4 + "\n")
                    
def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:          
            processed_line = line.upper()
            outfile.write(processed_line + '\n')
            
def load():
    global transformed_file, output_file
    print("Inside Load")
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')

def check():
    global output_file
    print("Inside Check")
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)

default_args = {
    "owner": "Nititorn Kitprasopchok",
    "email": "ampamp0009@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_Server_Access_Log_Processing",
    default_args=default_args,
    description="A simple ETL DAG for processing server access logs",
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    
    # Define the task named download to call the `download_file` function
    execute_download = PythonOperator(
        task_id='download',
        python_callable=download_file,
        dag=dag,
    )

    # Define the task named execute_extract to call the `extract` function
    execute_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        dag=dag,
    )

    # Define the task named execute_transform to call the `transform` function
    execute_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        dag=dag,
    )

    # Define the task named execute_load to call the `load` function
    execute_load = PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag,
    )

    # Define the task named execute_load to call the `load` function
    execute_check = PythonOperator(
        task_id='check',
        python_callable=check,
        dag=dag,
    )

# Task pipeline
execute_download >> execute_extract >> execute_transform >> execute_load >> execute_check
    
# download_file()
# extract()
# transform()
# load()
# check()