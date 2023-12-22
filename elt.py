import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime as dt
import sqlite3


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    # Initial dataframe
    df = pd.DataFrame(columns=table_attribs)

    # Get and parse HTML page
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    
    tables = soup.find_all(name="tbody")
    rows = tables[2].find_all(name="tr")

    for row in rows:
        col = row.find_all(name="td")
        if len(col) != 0:
            country = col[0].find_all(name="a")
            gdp_usd_millions = col[2].text

            if len(country) != 0 and gdp_usd_millions != "—":
                dict = {
                    "Country": country[0].text,
                    "GDP_USD_millions": gdp_usd_millions
                }

                df = pd.concat([df, pd.DataFrame([dict])], ignore_index=True)

    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    transformed_df = df.copy()

    transformed_df["GDP_USD_millions"] = transformed_df["GDP_USD_millions"].apply(lambda x: float("".join(x.split(","))))
    transformed_df["GDP_USD_millions"] = round(transformed_df["GDP_USD_millions"] / 1000, 2)
    transformed_df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"}, inplace=True)

    return transformed_df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index= False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    result = pd.read_sql(query_statement, sql_connection)
    print(f"\n{query_statement}\n")
    print(result)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = dt.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}: {message}\n")


def process():
    sql_connection = sqlite3.connect(db_name)

    # Log the initialization of the ETL process 
    log_progress("ETL Job Started") 
    
    # Log the beginning of the Extraction process 
    log_progress("Extract phase Started") 
    extracted_data = extract(url, table_attribs) 
    
    # Log the completion of the Extraction process 
    log_progress("Extract phase Ended") 
    
    # Log the beginning of the Transformation process 
    log_progress("Transform phase Started") 
    transformed_data = transform(extracted_data) 
    print("Transformed Data") 
    print(transformed_data) 
    
    # Log the completion of the Transformation process 
    log_progress("Transform phase Ended") 
    
    # Log the beginning of the Loading process 
    log_progress("Load phase Started") 
    load_to_csv(transformed_data, csv_path)
    load_to_db(transformed_data, sql_connection, table_name)
    
    # Log the completion of the Loading process 
    log_progress("Load phase Ended") 
    
    # Log the completion of the ETL process 
    log_progress("ETL Job Ended")

    # Log the beginning of running the query
    log_progress("Running the query Started")
    query_statement = f"select * from {table_name} where GDP_USD_billions >= 100"
    run_query(query_statement, sql_connection)

    # Log the completion of running the query
    log_progress("Running the query Ended")

    # Log the completion of process
    log_progress("Process Complete.")

    sql_connection.close()


if __name__ == "__main__":
    url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    table_attribs = ["Country", "GDP_USD_millions"]
    db_name = "World_Economies.db"
    table_name = "Countries_by_GDP"
    csv_path = "Countries_by_GDP.csv"
    log_file = "log.txt"
    log_progress("Preliminaries complete. Initiating ETL process")

    extracted_data = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")

    transformed_data = transform(extracted_data)
    log_progress("Data transformation complete. Initiating Loading process")

    load_to_csv(transformed_data, csv_path)
    log_progress("Data saved to CSV file")

    sql_connection = sqlite3.connect(db_name)
    log_progress("SQL Connection initiated")

    load_to_db(transformed_data, sql_connection, table_name)
    log_progress("Data loaded to Database as a table, Executing queries")
    
    query_statement = f"select * from {table_name} where GDP_USD_billions >= 100"
    run_query(query_statement, sql_connection)

    log_progress("Process Complete")

    sql_connection.close()
    log_progress("Server Connection closed")
    