"""
* Code for ETL operations on Global-Bank-Data
* Created: 8/19/2024 5:35:58 AM
* Author : Omar El-Azab
"""

import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

data_url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_file_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
table_attributes = ["Name", "MC_USD_Billion"]
table_attributes_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]

def log_progress(message):
    timestampformat = '%Y-%h-%d-%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestampformat)
    with open ('./code_log.txt','a') as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    response = requests.get(url)
    if response.status_code == 200:
        web_page = response.text
    web_soup = BeautifulSoup(web_page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)

    web_table = web_soup.find_all("table",{'class':'wikitable'})
    table_rows = web_table[0].find_all("tr")
    for row in table_rows:
        col = row.find_all("td")
        if len(col) !=0:
            if col[1].find('a') is not None and '—' not in col[2]:
                data_dict = {"Name": col[1].find_all('a')[1]['title'],
                             "MC_USD_Billion": float(col[2].contents[0][:-1])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    dataframe = pd.read_csv('exchange_rate.csv')
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(data_url, table_attributes)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_file_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = input()
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
query_statement = input()
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
query_statement = input()
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
