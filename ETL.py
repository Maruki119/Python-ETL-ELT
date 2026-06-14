import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—'not in col[2]:
                data_dict = {'Country': col[0].a.contents[0],
                             'GDP_USD_millions': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df):
    #print(df)
    df["GDP_USD_millions"] = (
        df["GDP_USD_millions"]
        .str.replace(",", "", regex=False)
        .astype(float)
        )

    df["GDP_USD_millions"] = np.round(df["GDP_USD_millions"] / 1000, 2)

    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# ETL Process

try:
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    table_attribs = ["Country", "GDP_USD_millions"]
    db_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'
    csv_path = 'Countries_by_GDP.csv'

    print('Start Extract data')
    log_progress('Start Extract data')
    extract_data = extract(url, table_attribs)
    log_progress('End Extract data')
    print('End Extract data')

    print('Start Transform data')
    log_progress('Start Transform data')
    transform_data = transform(extract_data)
    log_progress('End Transform data')
    print('End Transform data')

    print('Start Load data to csv')
    log_progress('Start Load data to csv')
    load_to_csv(transform_data, csv_path)
    log_progress('End Load data to csv')
    print('End Load data to csv')

    print('Connect to DB')
    log_progress('Connect to DB')
    conn = sqlite3.connect(db_name)
    log_progress('DB is connected')
    print('DB is connected')

    print('Start Load data to db')
    log_progress('Start Load data to db')
    load_to_db(transform_data, conn, table_name)
    log_progress('End Load data to db')
    print('End Load data to db')

    print('Start Query from db')
    log_progress('Start Query from db')
    run_query(f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100", conn)
    log_progress('End Query from db')
    print('End Query from db')

    log_progress('Process Complete.')

except Exception as e:
    print(f"\n[PROCESS FAILED] ETL process stopped because of error: {e}")

    try:
        log_progress(f"Process Failed: {e}")
    except:
        print("[ERROR] Could not write failure message to log file.")

finally:
    try:
        conn.close()
        print("Database connection closed.")
    except NameError:
        print("Database connection was not created.")
    except Exception as e:
        print(f"[ERROR] Failed to close database connection: {e}")