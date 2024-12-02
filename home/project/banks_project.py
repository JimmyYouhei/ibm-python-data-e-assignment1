# Code for ETL operations on Country-GDP data
# Importing the required libraries
import datetime
import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
import sqlite3




def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open('code_log.txt', 'a') as log_file:
        time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{time_stamp} : {message}\n")
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table under the heading "By market capitalization"
    table = soup.find('table', {'class': 'wikitable'})

    # Read the table into a pandas DataFrame
    df = pd.read_html(str(table))[0]

    # Clean the Market Cap column
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].replace('\n', '').astype(float)

    # Rename columns to match the required attributes
    df.columns = table_attribs

    log_progress("Data extraction complete. Initiating Transformation process")
    return df

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank' , 'Name', 'MC_USD_Billion']
df = extract(url, table_attribs)
# print(df)

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate_df = pd.read_csv(csv_path)

    # Convert the contents to a dictionary
    exchange_rate = dict(zip(exchange_rate_df.iloc[:, 0], exchange_rate_df.iloc[:, 1]))

    # Add new columns to the DataFrame
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Transformation process complete")
    return df

csv_path = 'exchange_rate.csv'
transformed_df = transform(df, csv_path)
print(transformed_df.to_string())

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

output_path = 'banks_market_cap.csv'
load_to_csv(transformed_df, output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

# Initiate the connection to the SQLite3 database server
db_connection = sqlite3.connect('Banks.db')

# Call the function to load the transformed data frame to the SQL database
load_to_db(transformed_df, db_connection, 'Largest_banks')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    print(f"Query: {query_statement}")
    for row in results:
        print(row)
    log_progress("Query executed and results printed")
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Initiate the connection to the SQLite3 database server
db_connection = sqlite3.connect('Banks.db')

# Execute the queries
query1 = "SELECT * FROM Largest_banks"
run_query(query1, db_connection)

query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, db_connection)

query3 = "SELECT Name from Largest_banks LIMIT 5"
run_query(query3, db_connection)

