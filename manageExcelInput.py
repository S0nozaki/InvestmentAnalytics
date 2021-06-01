import yaml
import sqlite3 as sql
import pandas as pd

with open("config.yml", "r") as file_descriptor:
    config = yaml.safe_load(file_descriptor)
    db = config["db"]
    table = config["transaction_table"]
    date = config["date"]
    columns_selected_for_db = config["columns_selected_for_db"]
    ticker = config["ticker"]
    currency = config["currency"]
    currencies = config["currencies"]
    files_and_currencies = config["files_and_currencies"]


def open_file_connection(path):
    xls_dataframe = pd.read_excel(path)
    return xls_dataframe


def drop_empty_rows(dataframe, column_to_filter):
    return dataframe.dropna(subset=[column_to_filter])


def sort_by_date(dataframe):
    dataframe[date] = pd.to_datetime(dataframe[date], format='%d/%m/%y')
    dataframe = dataframe.sort_values(by=[date])
    dataframe[date] = dataframe[date].dt.strftime('%d/%m/%y')
    return dataframe


def create_dataframe():
    dataframe = pd.DataFrame()
    for filepath, file_currency in files_and_currencies.items():
        file_dataframe = open_file_connection(filepath)
        file_dataframe[currency] = currencies[file_currency]
        dataframe = dataframe.append(file_dataframe)
    return sort_by_date(drop_empty_rows(dataframe[columns_selected_for_db], ticker))


def create_transaction_table():
    conn = sql.connect(db)
    create_dataframe().reset_index(drop=True).to_sql(table, conn)
    conn.close()
