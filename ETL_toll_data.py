from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

source_dir = "$(pwd)/opt/airflow/dags/finalassignment/"
destination_dir = "$(pwd)/opt/airflow/dags/finalassignment"

default_args = {
    "owner": "Nititorn Kitprasopchok",
    "email": "nititorn.kij@gmail.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule=timedelta(days=1),
) as dag:
    
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command="tar -xvf " + source_dir + "/tolldata.tgz -C " + destination_dir,
        dag=dag
    )
    
    extract_data_from_csv = BashOperator(
        task_id="extract_data_from_csv",
        bash_command=f"""
            echo "Rowid,Timestamp,Anonymized Vehicle Number,Vehicle Type" > {destination_dir}/csv_data.csv
            cut -d',' -f1-4 {destination_dir}/vehicle-data.csv >> {destination_dir}/csv_data.csv
        """,
        dag=dag
    )
    
    extract_data_from_tsv = BashOperator(
        task_id="extract_data_from_tsv",
        bash_command=f"""
            echo "Number of axles,Tollplaza id, Tollplaza code" > {destination_dir}/tsv_data.tsv
            cut -d$'\t' -f5-7 {destination_dir}/tollplaza-data.tsv | tr '\t' ',' >> {destination_dir}/tsv_data.tsv
        """,
        dag=dag
    )
    
    extract_data_from_fixed_width = BashOperator(
        task_id="extract_data_from_fixed_width",
        bash_command=f"""
            echo "Type of Payment code, Vehicle Code" > {destination_dir}/fixed_width_data.csv
            awk '{{print substr($0,59,3) "," substr($0,63,5)}}' \
            {destination_dir}/payment-data.txt \
            > {destination_dir}/fixed_width_data.csv
        """,
        dag=dag
    )
    
    consolidate_data = BashOperator(
        task_id="consolidate_data",
        bash_command=f"""
            echo "Rowid,Timestamp,Anonymized Vehicle Number,Vehicle Type,Number of axles,Tollplaza id,Tollplaza code,Type of Payment code, Vehicle Code" > {destination_dir}/extracted_data.csv
            paste -d',' {destination_dir}/csv_data.csv {destination_dir}/tsv_data.tsv {destination_dir}/fixed_width_data.csv >> {destination_dir}/extracted_data.csv
        """,
        dag=dag
    )
    
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=f"""
            awk -F',' 'BEGIN {{OFS=","}} NR==1 {{print; next}} {{$4=toupper($4); print}}' \
            {destination_dir}/extracted_data.csv \
            > {destination_dir}/staging/transformed_data.csv
        """,
        dag=dag
    )
    
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data