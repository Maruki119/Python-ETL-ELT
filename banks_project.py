import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Configurations

url_bank = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
url_exchange = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_path = './code_log.txt'

# Functions

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_path,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        
def extract(url, table_attribs):
    page = requests.get(url)
    data = BeautifulSoup(page.text, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
   
    for row in rows:
        col = row.find_all('td')
        
        if len(col) != 0:
            
            if len(col) >= 3:
                links = col[1].find_all('a')
                
                data_dict = {
                    "Name": links[-1].contents[0],
                    "MC_USD_Billion": col[2].contents[0]
                }

                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                    
    print("Extracted Data:\n", df)
    
    return df

def transform(df, df_exchange):
    df["MC_USD_Billion"] = (
        df["MC_USD_Billion"]
        .str.replace(",", "", regex=False)
        .astype(float)
        )
    
    eur = df_exchange.loc[df_exchange['Currency'] == 'EUR', 'Rate'].values[0]
    gbp = df_exchange.loc[df_exchange['Currency'] == 'GBP', 'Rate'].values[0]
    inr = df_exchange.loc[df_exchange['Currency'] == 'INR', 'Rate'].values[0]
    
    df = df.assign(
        MC_USD_Billion = np.round(df["MC_USD_Billion"], 2),
        MC_EUR_Billion = np.round(df["MC_USD_Billion"] * eur, 2),
        MC_GBP_Billion = np.round(df["MC_USD_Billion"] * gbp, 2),
        MC_INR_Billion = np.round(df["MC_USD_Billion"] * inr, 2)
    )
    
    print("Transformed Data:\n", df)
    
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    print(f"Data loaded to CSV at {csv_path}")
    
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    print(f"Data loaded to database table {table_name}")
    log_progress(f"Data loaded to database table {table_name}")

def run_query(query_statement, sql_connection):
    print(f"Running query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print("Query Output:\n", query_output)

# Download exchange_rate.csv

exchange_rate_df = requests.get(url_exchange, stream=True)
if exchange_rate_df.status_code == 200:
    with open('exchange_rate.csv', 'wb') as f:
        for chunk in exchange_rate_df.iter_content(chunk_size=8192):
            f.write(chunk)
        print('Downloaded exchange_rate.csv successfully.')
        log_progress('Downloaded exchange_rate.csv successfully.')
else:
    print(f'Failed to download exchange_rate.csv. Status code: {exchange_rate_df.status_code}')
    log_progress(f'Failed to download exchange_rate.csv. Status code: {exchange_rate_df.status_code}')
    
try:
    df_exchange = pd.read_csv('exchange_rate.csv')
    log_progress('Read exchange_rate.csv successfully.')
    print(df_exchange)
except Exception as e:
    print(f'Error reading exchange_rate.csv: {e}')
    log_progress(f'Error reading exchange_rate.csv: {e}')

# ETL Process

try:
    # Log the start of the ETL process
    print('Start of ETL process')
    log_progress('Start of ETL process')
    
    # Extract data
    print('Start Extract data')
    log_progress('Start Extract data')
    extract_data = extract(url_bank, table_attribs)
    log_progress('End Extract data')
    print('End Extract data')

    # Transform data
    print('Start Transform data')
    log_progress('Start Transform data')
    transform_data = transform(extract_data, df_exchange)
    log_progress('End Transform data')
    print('End Transform data')
    
    # Load data to csv
    print('Start Load data to csv')
    log_progress('Start Load data to csv')
    load_to_csv(transform_data, csv_path)
    log_progress('End Load data to csv')
    print('End Load data to csv')
    
    # Connect to DB
    print('Connect to DB')
    log_progress('Connect to DB')
    conn = sqlite3.connect(db_name)
    log_progress('DB is connected')
    print('DB is connected')
    
    # Load data to db
    print('Start Load data to db')
    log_progress('Start Load data to db')
    load_to_db(transform_data, conn, table_name)
    log_progress('End Load data to db')
    print('End Load data to db')
    
    # Query from db
    print('Start Query from db')
    log_progress('Start Query from db')
    run_query(f"SELECT * from {table_name}", conn)
    run_query(f"SELECT AVG(MC_GBP_Billion) from {table_name}", conn)
    run_query(f"SELECT name from {table_name} LIMIT 5", conn)
    log_progress('End Query from db')
    print('End Query from db')
    
finally:
    # Log the end of the ETL process
    print('End of ETL process')
    log_progress('End of ETL process')