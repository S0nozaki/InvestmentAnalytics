import yaml
import sqlite3 as sql
import pandas as pd

with open("config.yml", "r") as file_descriptor:
    config = yaml.safe_load(file_descriptor)
    sourceExcelFile = config["file"]
    db = config["db"]
    table = config["table"]
    columnsSelectedForDBList = config["columnsSelectedForDBList"]
    ticker = config["ticker"]
    currency = config["currency"]
    ARS = config["ARS"]

def open_file_connection():
    xls = pd.read_excel(sourceExcelFile)
    return xls

def drop_empty_rows(dataframe, columnToFilter):
    return dataframe.dropna(subset=[columnToFilter])

def create_dataframe():
    file = open_file_connection()
    file[currency] = ARS
    return drop_empty_rows(file[columnsSelectedForDBList],ticker)

def create_db():
    conn = sql.connect(db)
    create_dataframe().reset_index(drop=True).to_sql(table, conn)
    conn.close()

def read_from_db():
    conn = sql.connect(db)
    c = conn.cursor()
    c.execute("SELECT * FROM " + table)
    for row in c.fetchall():
        print(row)
    c.close()
    conn.close()