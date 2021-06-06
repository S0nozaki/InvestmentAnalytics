import pandas as pd
import sqlite3 as sql
import locale
import yaml
from decimal import Decimal

locale.setlocale(locale.LC_ALL, '')

with open("config.yml", "r") as file_descriptor:
    config = yaml.safe_load(file_descriptor)
    db = config["db"]
    transaction_table = config["transaction_table"]
    exchange_table = config["dolar_table"]
    excluded_tickers = config["excluded_tickers"]
    date = config["date"]
    quantity = config["quantity"]
    ticker = config["ticker"]
    price = config["price"]
    value = config["value"]
    currency = config["currency"]
    local_currency = config["local_currency"]
    reference_currency = config["reference_currency"]


def get_dataframe(table):
    conn = sql.connect(db)
    df = pd.read_sql_query("SELECT * FROM '" + table["Name"] +
                           "'", conn, parse_dates={table["Date"]: '%d/%m/%y'})
    conn.close()
    return df


def filter_column(dataframe, column, filter):
    return dataframe[dataframe[column].isin(filter) == False]


def get_currencies(dataframe):
    return dataframe[currency].unique()


def get_price_per_currency(transaction, curr):
    exchange_rate = Decimal(str(transaction[exchange_table["Rate"]]))
    transaction_price = Decimal(str(transaction[price]))
    if(transaction[currency] == reference_currency) and (curr == local_currency):
        return str(transaction_price * exchange_rate)
    elif (transaction[currency] == local_currency) and (curr == reference_currency):
        return str(transaction_price / exchange_rate)
    return str(transaction_price)


def get_ticker_balance(selected_ticker):
    file = filter_column(get_dataframe(transaction_table),
                         ticker, excluded_tickers)
    exchange_rate = get_exchange_dataframe()
    transactions = file.loc[file[ticker] == selected_ticker]
    transactions = transactions.merge(exchange_rate)
    currencies = get_currencies(transactions)
    for curr in currencies:
        transactions[curr] = transactions.apply(
            lambda x: get_price_per_currency(x, curr), axis=1)
    result = dict.fromkeys(currencies, 0)
    positions = []
    total_position = 0
    for index, transaction in transactions.iterrows():
        quantity_to_process = transaction[quantity]
        while quantity_to_process != 0:
            # if there are no existing positions or if you can't close any operation
            if (not positions) or ((positions[0][quantity] > 0) == (transaction[quantity] > 0)):
                positions.append(create_position(
                    transaction[quantity], transaction[currencies]))
                total_position += quantity_to_process
                quantity_to_process = 0
                continue
            # if the transaction to process can't be closed completely by the oldest position
            if ((positions[0][quantity] > 0) != (positions[0][quantity] + quantity_to_process >= 0)):
                for curr in currencies:
                    result[curr] += abs(positions[0][quantity]) * \
                        (Decimal(transaction[curr]) -
                         Decimal(positions[0][curr]))
                quantity_to_process += positions[0][quantity]
                total_position -= positions[0][quantity]
                positions.pop(0)
            else:  # if the transaction can be closed completely
                for curr in currencies:
                    result[curr] += abs(quantity_to_process) * \
                        (Decimal(transaction[curr]) -
                         Decimal(positions[0][curr]))

                positions[0][quantity] += quantity_to_process
                if positions[0][quantity] == 0:
                    positions.pop(0)
                total_position += quantity_to_process
                quantity_to_process = 0
    return [result, total_position]


def create_position(quantity, currencies):
    position = {"Cantidad": quantity}
    for position_currency, currency_price in currencies.iteritems():
        position[position_currency] = currency_price
    return position


def get_exchange_dataframe():
    df = get_dataframe(exchange_table)
    df = df.rename(columns={exchange_table["Date"]: transaction_table["Date"]})
    return df[[transaction_table["Date"], exchange_table["Rate"]]]


def get_total_metadata():
    total = filter_column(get_dataframe(
        transaction_table), ticker, excluded_tickers)
    tickers = total[ticker].unique()
    currencies_counter = dict.fromkeys(get_currencies(total), 0)
    return [tickers, currencies_counter]


def filter_by_year(year):
    file = filter_column(get_dataframe(transaction_table),
                         ticker, excluded_tickers)
    return file.loc[file[date].dt.year == year].reset_index(drop=True)
