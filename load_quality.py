import pandas as pd
import sys
import datetime
import psycopg
import credentials


def data_handle(df):
    # Converting NA
    replacement = {"Not Available": None}
    df.replace(replacement, inplace=True)
    return df


def run_sql(df):
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()

    num_rows_hospital_insert = 0
    num_rows_hospital_update = 0
    with conn.transaction():
        for index, row in df.iterrows():
            try:
                cur.execute("INSERT INTO Hospital (hospital_pk,\
                        type, ownership, emergency_services, county_name, state)"
                                "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')".format
                                (row['Facility ID'], row['Hospital Type'],
                                row['Hospital Ownership'], row['Emergency Services'],
                                row['County Name'], row['State']))
            except Exception:
                try:
                    cur.execute("UPDATE Hospital "
                                    "SET type = '{0}',\
                                    ownership = '{1}',\
                                    emergency_services = '{2}',\
                                    county_name = '{3}',\
                                    state = '{4}'"
                                    "WHERE hospital_pk = '{5}'".format
                                    (row['Hospital Type'], row['Hospital Ownership'],
                                        row['Emergency Services'], row['County Name'],
                                        row['State'], row['Facility ID']))
                except Exception as e:
                    print("insert and update failed:", e)
                else:
                    num_rows_hospital_update += 1
            else:
                num_rows_hospital_insert += 1
    print("Info about", num_rows_hospital_insert, "hospitals are inserted.")
    print("Info about", num_rows_hospital_update, "hospitals are updated.")

    num_rows_rating = 0
    with conn.transaction():
        for index, row in df.iterrows():
            try:
                cur.execute("INSERT INTO Rating (day, rating, hospital)"
                                "VALUES (CAST('{0}' AS DATE), {1}, '{2}')".format
                                (row['quality_date'], row['Hospital overall rating'],
                                row['Facility ID']))
            except Exception as e:
                print("insert failed:", e)
            else:
                num_rows_rating += 1
    print(num_rows_rating, "rating info are inserted.")


    conn.commit()
    conn.close()


# Load data
quality_date = sys.argv[1]
quality_date = datetime.datetime.strptime(quality_date, '%Y-%m-%d').date()
path_name = sys.argv[2]
df = pd.read_csv(path_name)
# Handling data
df = data_handle(df)
# Insert the date of the quality data
df['quality_date'] = quality_date
# SQL
run_sql(df[:2])
