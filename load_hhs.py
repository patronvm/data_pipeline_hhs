import pandas as pd
import sys
import psycopg2


def data_handle(df):
    # Converting NA
    replacement = {-999999: None}
    df.replace(replacement, inplace=True)

    # Parsing dates
    df['collection_week'] = df['collection_week'].astype('datetime64[ns]')

    # Converting points into longitude and latitude
    df['geocoded_hospital_address'].fillna("POINT (NA NA)", inplace=True)
    df['longitude'] = df['geocoded_hospital_address']\
        .map(lambda x: x.split()[1][1:])
    df['latitude'] = df['geocoded_hospital_address']\
        .map(lambda x: x.split()[2][:-1])
    df['geocoded_hospital_address'].replace("POINT (NA NA)", None,
                                            inplace=True)
    df['longitude'].replace("NA", None, inplace=True)
    df['latitude'].replace("NA", None, inplace=True)
    return df


def run_sql():
    pass


# Load data
path_name = sys.argv[1]
df = pd.read_csv(path_name)
df = data_handle(df)
# SQL
run_sql()
