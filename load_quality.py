import pandas as pd
import sys
import datetime
import psycopg
import credentials


def isint(num):
    try:
        int(num)
        return True
    except:
        return False


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
    invalid_rating_id = []
    with conn.transaction():
        for index, row in df.iterrows():
            try:
                if_invalid = False
                rating_dict = {"day": row["quality_date"],
                "rating": row['Hospital overall rating'],
                "hospital": row['Facility ID']}

                nonnull_dict = {}
                for key in rating_dict.keys():
                    if rating_dict[key] != None:
                        nonnull_dict[key] = rating_dict[key]

                # Check if the value is valid or not
                # If the value is invalid, skip inserting this row
                for key in nonnull_dict.keys():
                    if key == "day":
                        if type(nonnull_dict[key]) != datetime.date:
                            invalid_rating_id.append(row['Facility ID'])
                            if_invalid = True
                            break
                    elif key == "rating":
                        if not isint(nonnull_dict[key]) or int(nonnull_dict[key]) <= 0:
                            invalid_rating_id.append(row['Facility ID'])
                            if_invalid = True
                            break
                if if_invalid:
                    print("Row invalid, Facility ID:", row['Facility ID'])
                    continue

                # Insert valid rows
                sql_col = ', '.join(nonnull_dict.keys())
                sql_insert = "INSERT INTO Rating (" + sql_col + ") " +\
                    "VALUES ("

                for key in list(nonnull_dict.keys()):
                    if key == "day":
                        insert = "CAST('{}' AS DATE)".format(row['quality_date'])
                    elif key == "hospital":
                        insert = "'" + str(nonnull_dict[key]) + "'"
                    else:
                        insert = str(nonnull_dict[key])
                    sql_insert += insert
                    if key != list(nonnull_dict.keys())[-1]:
                        sql_insert += ", "
                    else:
                        sql_insert += ")"
                cur.execute(sql_insert)

            except Exception as e:
                print("insert failed:", e)
            else:
                num_rows_rating += 1
    print(num_rows_rating, "rating info are inserted.")


    conn.commit()
    conn.close()
    return invalid_rating_id


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
invalid_rating_id = run_sql(df[:2])
# Save invalid rows to a separate CSV file
invalid_rows = pd.DataFrame()
for id in invalid_rating_id:
    row = df[df["Facility ID"]==id]
    invalid_rows = pd.concat([invalid_rows, row])
invalid_rows.to_csv("Invalid Rows_quality.csv")