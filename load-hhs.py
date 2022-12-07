import pandas as pd
import sys
import psycopg
import credentials
import datetime



def isfloat(num):
    """Justify if the given number is a float"""
    try:
        float(num)
        return True
    except (ValueError, TypeError):
        return False


def isint(num):
    """Justify if the given number is an integer"""

    try:
        int(num)
        return True
    except (ValueError, TypeError):
        return False


def data_handle(df):
    # Converting NA
    replacement = {-999999: None}
    df.replace(replacement, inplace=True)

    # Parsing dates
    df['collection_week'] = pd.to_datetime(df['collection_week'], format='%m/%d/%y')

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
    df = df.where(pd.notnull(df), None)

    return df


def run_sql(df):
    """Run SQL inseration and update and returns invalid row id
    """

    # Connects to the database with given credentials info
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()

    invalid_hospital_id = []
    num_rows_hospital_insert = 0
    num_rows_hospital_update = 0
    with conn.transaction():
        for index, row in df.iterrows():
            if_invalid = False
            hospital_dict = {'hospital_pk': row['hospital_pk'],
                             'hospital_name': row['hospital_name'],
                             'longitude': row['longitude'],
                             'latitude': row['latitude'],
                             'address': row['address'],
                             'city': row['city'],
                             'fips_code': row['fips_code'],
                             'zip': row['zip']}
            nonnull_hospital = {}
            for key in hospital_dict.keys():
                if hospital_dict[key] is not None:
                    nonnull_hospital[key] = hospital_dict[key]

            # Check if the values are valid
            for key in nonnull_hospital.keys():
                if key == 'longitude':
                    if not isfloat(nonnull_hospital[key]) or\
                            float(nonnull_hospital[key]) <= -180 or\
                            float(nonnull_hospital[key]) >= 180:
                        invalid_hospital_id.append(row["hospital_pk"])
                        if_invalid = True
                        break
                elif key == 'latitude':
                    if not isfloat(nonnull_hospital[key]) or\
                            float(nonnull_hospital[key]) <= -90 or\
                            float(nonnull_hospital[key]) >= 90:
                        invalid_hospital_id.append(row["hospital_pk"])
                        if_invalid = True
                        break
                elif key == "zip":
                    if not isint(nonnull_hospital[key]):
                        invalid_hospital_id.append(row["hospital_pk"])
                        if_invalid = True
                        break

            if if_invalid:
                print("Row invalid, hospital_pk:", row['hospital_pk'])
                continue

            try:
                insert_col = ', '.join(nonnull_hospital.keys())
                hospital_insert = "INSERT INTO Hospital (" +\
                                  insert_col + ")" +\
                                  "VALUES ("
                for key in list(nonnull_hospital.keys()):
                    if key in ["hospital_pk", "hospital_name",
                               "address", "city", "fips_code"]:
                        if key in ["hospital_name",
                                   "address", "city"] and\
                                   "'" in nonnull_hospital[key]:
                            name = nonnull_hospital[key]
                            nonnull_hospital[key] = name.split("'")[0] +\
                                "''" + name.split("'")[1]

                        key_insert = "'" + str(nonnull_hospital[key]) + "'"

                    else:
                        key_insert = str(nonnull_hospital[key])
                    hospital_insert += key_insert
                    if key != list(nonnull_hospital.keys())[-1]:
                        hospital_insert += ", "
                    else:
                        hospital_insert += ");"
                cur.execute(hospital_insert)
            except Exception:
                pass
            else:
                num_rows_hospital_insert += 1


    if num_rows_hospital_insert == 0: 
        with conn.transaction():
            for index, row in df.iterrows():
                if_invalid = False
                hospital_dict = {'hospital_pk': row['hospital_pk'],
                                'hospital_name': row['hospital_name'],
                                'longitude': row['longitude'],
                                'latitude': row['latitude'],
                                'address': row['address'],
                                'city': row['city'],
                                'fips_code': row['fips_code'],
                                'zip': row['zip']}
                nonnull_hospital = {}
                for key in hospital_dict.keys():
                    if hospital_dict[key] is not None:
                        nonnull_hospital[key] = hospital_dict[key]

                # Check if the values are valid
                for key in nonnull_hospital.keys():
                    if key == 'longitude':
                        if not isfloat(nonnull_hospital[key]) or\
                                float(nonnull_hospital[key]) <= -180 or\
                                float(nonnull_hospital[key]) >= 180:
                            invalid_hospital_id.append(row["hospital_pk"])
                            if_invalid = True
                            break
                    elif key == 'latitude':
                        if not isfloat(nonnull_hospital[key]) or\
                                float(nonnull_hospital[key]) <= -90 or\
                                float(nonnull_hospital[key]) >= 90:
                            invalid_hospital_id.append(row["hospital_pk"])
                            if_invalid = True
                            break
                    elif key == "zip":
                        if not isint(nonnull_hospital[key]):
                            invalid_hospital_id.append(row["hospital_pk"])
                            if_invalid = True
                            break

                if if_invalid:
                    print("Row invalid, hospital_pk:", row['hospital_pk'])
                    continue
                
                try:
                        hospital_update = "UPDATE Hospital SET "
                        for key in list(nonnull_hospital.keys()):
                            if key in ["city", "fips_code"]:
                                key_update = key + " = '" +\
                                            str(nonnull_hospital[key]) + "'"
                            elif key in ["hospital_name", "address", "city"]:
                                if "'" in nonnull_hospital[key]:
                                    name = nonnull_hospital[key]
                                    nonnull_hospital[key] = name.split("'")[0] +\
                                        "''" + name.split("'")[1]
                                    key_update = key + " = '" +\
                                        str(nonnull_hospital[key]) + "'"
                                else:
                                    key_update = key + " = '" +\
                                                str(nonnull_hospital[key]) + "'"
                            elif key != "hospital_pk":
                                key_update = key + " = " +\
                                            str(nonnull_hospital[key])
                            else:
                                key_update = ""
                            hospital_update += key_update
                            if key != list(nonnull_hospital.keys())[-1]\
                            and key != "hospital_pk":
                                hospital_update += ", "
                            if key == "hospital_pk":
                                pk_update = " WHERE hospital_pk = '" +\
                                            str(nonnull_hospital[key]) + "';"
                        hospital_update += pk_update
                        cur.execute(hospital_update)
                except Exception as e:
                        print("insert and update failed:", e)
                else:
                        num_rows_hospital_update += 1

    print("Info about", num_rows_hospital_insert, "hospitals are inserted.")
    print("Info about", num_rows_hospital_update, "hospitals are updated.")

    invalid_beds_id = []
    num_rows_beds = 0
    with conn.transaction():
        for index, row in df.iterrows():
            if_invalid = False
            try:
                select_sql = "SELECT COUNT(*) FROM Hospital WHERE hospital_pk = '{}'".format(row['hospital_pk'])
                cur.execute(select_sql)
                for r in cur:
                    pk_exist = r
                r = cur.fetchone()
                pk_num = pk_exist[0]
            except Exception:
                pk_num = 0

            if pk_num != 0:
                try:
                    beds_dict = {'all_adult_hospital_beds_'
                                '7_day_avg': row['all_adult'
                                                '_hospital_beds_7_day_avg'],
                                'all_pediatric_inpatient_beds'
                                '_7_day_avg': row['all_pediatric_'
                                                'inpatient_beds_7_day_avg'],
                                'all_adult_hospital_inpatient_bed_occupied_7'
                                '_day_coverage': row['all_adult_hospital'
                                                    '_inpatient_bed_'
                                                    'occupied_7_day_coverage'],
                                'all_pediatric_inpatient_bed_occupied_'
                                '7_day_avg': row['all_pediatric_inpatient'
                                                '_bed_occupied_7_day_avg'],
                                'total_icu_beds_7_day_avg': row['total_icu'
                                                                '_beds_7_day_'
                                                                'avg'],
                                'icu_beds_used_7_'
                                'day_avg': row['icu_beds_used_7_day_avg'],
                                'inpatient_beds_used_covid_7_'
                                'day_avg': row['inpatient_beds_used_'
                                                'covid_7_day_avg'],
                                'staffed_adult_icu_patients_confirmed'
                                '_covid_7_day_avg': row['staffed_icu_adult_'
                                                        'patients_confirmed'
                                                        '_covid_7_day_avg']}

                    # Collect non-null values
                    nonnull_dict = {}
                    for key in beds_dict.keys():
                        if beds_dict[key] is not None:
                            nonnull_dict[key] = beds_dict[key]

                    # Check if the value is valid or not
                    # If the value is invalid, skip inserting this row
                    for key in nonnull_dict.keys():
                        value = nonnull_dict[key]
                        if not isfloat(value):
                                invalid_beds_id.append(row['hospital_pk'])
                                if_invalid = True
                                break
                    if if_invalid:
                        print("Row invalid, hospital_pk:", row['hospital_pk'])
                        continue

                    # Insert valid rows
                    sql_col = ', '.join(nonnull_dict.keys())
                    sql_value_str = map(str, nonnull_dict.values())
                    sql_value = ', '.join(sql_value_str)
                    sql_insert = "INSERT INTO Hospital_beds (" + sql_col +\
                                ", hospital, collection_week)" +\
                                "VALUES (" + sql_value +\
                                ", '{}', '{}');".format(row['hospital_pk'], row['collection_week'])
                    cur.execute(sql_insert)
                except Exception as e:
                    pass
                else:
                    num_rows_beds += 1
    
    print("Info about", num_rows_beds, "hospital beds are inserted.")
    conn.commit()
    conn.close()
    return invalid_hospital_id, invalid_beds_id


# Load data
path_name = sys.argv[1]
df = pd.read_csv(path_name)
df = data_handle(df)
# SQL
invalid_hospital_id, invalid_beds_id = run_sql(df[0:2])
# Save invalid rows to a separate CSV file
invalid_rows = pd.DataFrame()
for id in invalid_hospital_id:
    row = df[df["hospital_pk"] == id]
    invalid_rows = pd.concat([invalid_rows, row])
for id in invalid_beds_id:
    row = df[df["hospital_pk"] == id]
    invalid_rows = pd.concat([invalid_rows, row])

invalid_rows.to_csv("invalid_rows_hhs.csv")
