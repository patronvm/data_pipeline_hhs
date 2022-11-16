import pandas as pd
import sys
import datetime
import psycopg2


def data_handle(df):
    # Converting NA
    replacement = {"Not Available": None}
    df.replace(replacement, inplace=True)
    return df


def run_sql():
    pass


# Load data
quality_date = sys.argv[1]
quality_date = datetime.datetime.strptime(quality_date, '%Y-%m-%d').date()
path_name = sys.argv[2]
df = pd.read_csv(path_name)
# Handling data
df = data_handle(df)
df['quality_date'] = quality_date
# SQL
run_sql()
