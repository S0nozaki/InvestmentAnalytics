import pandas as pd
import sqlite3 as sql
import locale
import yaml

locale.setlocale(locale.LC_ALL, '')

with open("config.yml", "r") as file_descriptor:
    config = yaml.safe_load(file_descriptor)
    db = config["db"]
    table = config["transaction_table"]
    excluded_tickers = config["excluded_tickers"]
    date = config["date"]
    quantity = config["quantity"]
    ticker = config["ticker"]
    value = config["value"]


def get_dataframe():
    conn = sql.connect(db)
    df = pd.read_sql_query("SELECT * FROM '" + table +
                           "'", conn, parse_dates={date: '%d/%m/%y'})
    return filter_column(df, ticker, excluded_tickers)


def filter_column(dataframe, column, filter):
    return dataframe[dataframe[column].isin(filter) == False]


def get_ticker_balance(selected_ticker):
    file = get_dataframe()
    transactions = file.loc[file[ticker] == selected_ticker]
    position = 0
    transaction_value = locale.atof('0,00')
    result = 0
    unfinished_transaction = 0

    for index, transaction in transactions.iterrows():
        if unfinished_transaction == 0:
            unfinished_transaction = 1
        position += int(locale.atof(transaction[quantity]))
        transaction_value += locale.atof(transaction[value])
        if position <= 0 and unfinished_transaction != 0:
            result += transaction_value
            transaction_value = 0
            unfinished_transaction = 0
    return [result, position]


def get_valid_tickers():
    return get_dataframe()[ticker].unique()


def filter_by_year(year):
    file = get_dataframe()
    return file.loc[file[date].dt.year == year].reset_index(drop=True)
