import pandas as pd
import sys
import psycopg
import credentials


def isfloat(num):
    try:
        float(num)
        return True
    except:
        return False


def isint(num):
    try:
        int(num)
        return True
    except:
        return False


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
    df = df.where(pd.notnull(df), None)
    return df


def run_sql(df):
    # Connect
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
            hospital_dict = {'hospital_pk': row['hospital_pk'], 'hospital_name': row['hospital_name'],
            'longitude': row['longitude'], 'latitude': row['latitude'], 'address': row['address'],
            'city': row['city'], 'fips_code': row['fips_code'], 'zip': row['zip']}
            nonnull_hospital = {}
            for key in hospital_dict.keys():
                if hospital_dict[key] != None:
                    nonnull_hospital[key] = hospital_dict[key]
            
            # Check if the values are valid
            for key in nonnull_hospital.keys():
                if key == 'longitude':
                    if not isfloat(nonnull_hospital[key]) or\
                    float(nonnull_hospital[key]) <= -180 or float(nonnull_hospital[key]) >= 180:
                        invalid_hospital_id.append(row["hospital_pk"])
                        if_invalid = True
                        break
                elif key == 'latitude':
                    if not isfloat(nonnull_hospital[key]) or\
                    float(nonnull_hospital[key]) <= -90 or float(nonnull_hospital[key]) >= 90:
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
                hospital_insert = "INSERT INTO Hospital (" + insert_col + ")" +\
                    "VALUES ("
                for key in list(nonnull_hospital.keys()):
                    if key in ["hospital_pk", "hospital_name", "address", "city", "fips_code"]:
                        if key in ["hospital_name", "address"] and "'" in nonnull_hospital[key]:
                            name = nonnull_hospital[key]
                            nonnull_hospital[key] = name.split("'")[0] + "''" + name.split("'")[1]

                        key_insert = "'" + str(nonnull_hospital[key]) + "'"
                        

                    else:
                        key_insert = str(nonnull_hospital[key])
                    hospital_insert += key_insert
                    if key != list(nonnull_hospital.keys())[-1]:
                        hospital_insert += ", "
                    else:
                        hospital_insert += ")"
                #print(hospital_insert)
                cur.execute(hospital_insert)
            except Exception as e:
                try:
                    hospital_update = "UPDATE Hospital SET "
                    for key in list(nonnull_hospital.keys()):
                        if key in ["city", "fips_code"]:
                            key_update = key + " = '" + str(nonnull_hospital[key]) + "'"
                        elif key in ["hospital_name", "address"]:
                            if "'" in nonnull_hospital[key]:
                                name = nonnull_hospital[key]
                                nonnull_hospital[key] = name.split("'")[0] + "''" + name.split("'")[1]
                                key_update = key + " = '" + str(nonnull_hospital[key]) + "'"
                            else:
                                key_update = key + " = '" + str(nonnull_hospital[key]) + "'"
                        elif key != "hospital_pk":
                            key_update = key + " = " + str(nonnull_hospital[key])
                        else:
                            key_update = ""
                        hospital_update += key_update
                        if key != list(nonnull_hospital.keys())[-1] and key != "hospital_pk":
                            hospital_update += ", "
                        if key == "hospital_pk":
                            pk_update = " WHERE hospital_pk = '" + str(nonnull_hospital[key]) + "'"
                    hospital_update += pk_update
                    cur.execute(hospital_update)
                except Exception as e:
                    print("insert and update failed:", e)
                else:
                    num_rows_hospital_update += 1
            else:
                num_rows_hospital_insert += 1
    print("Info about", num_rows_hospital_insert, "hospitals are inserted.")
    print("Info about", num_rows_hospital_update, "hospitals are updated.")

    invalid_beds_id = []
    num_rows_beds = 0
    with conn.transaction():
        for index, row in df.iterrows():
            if_invalid = False
            try:
                beds_dict = {'all_adult_hospital_beds_7_day_avg': row['all_adult_hospital_beds_7_day_avg'],
                'all_pediatric_inpatient_beds_7_day_avg': row['all_pediatric_inpatient_beds_7_day_avg'],
                'all_adult_hospital_inpatient_bed_occupied_7_day_coverage': row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'],
                'all_pediatric_inpatient_bed_occupied_7_day_avg': row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                'total_icu_beds_7_day_avg': row['total_icu_beds_7_day_avg'],
                'icu_beds_used_7_day_avg': row['icu_beds_used_7_day_avg'],
                'inpatient_beds_used_covid_7_day_avg': row['inpatient_beds_used_covid_7_day_avg'],
                'staffed_adult_icu_patients_confirmed_covid_7_day_avg': row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']}

                # Collect non-null values
                nonnull_dict = {}
                for key in beds_dict.keys():
                    if beds_dict[key] != None:
                        nonnull_dict[key] = beds_dict[key]

                # Check if the value is valid or not
                # If the value is invalid, skip inserting this row
                for value in nonnull_dict.values():
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
                sql_insert = "INSERT INTO Hospital_beds (" + sql_col + ", hospital)" +\
                    "VALUES (" + sql_value + ", '{}')".format(row['hospital_pk'])
                cur.execute(sql_insert)
            except Exception as e:
                print("insert failed:", e)
                invalid_beds_id.append(row['hospital_pk'])
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
invalid_hospital_id, invalid_beds_id = run_sql(df)
# Save invalid rows to a separate CSV file
invalid_rows = pd.DataFrame()
for id in invalid_hospital_id:
    row = df[df["hospital_pk"]==id]
    invalid_rows = pd.concat([invalid_rows, row])
for id in invalid_beds_id:
    row = df[df["hospital_pk"]==id]
    invalid_rows = pd.concat([invalid_rows, row])
invalid_rows.to_csv("Invalid Rows_hhs.csv")