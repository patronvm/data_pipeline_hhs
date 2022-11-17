import pandas as pd
import sys
import psycopg
import credentials


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


def run_sql(df):
    # Connect
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
                cur.execute("INSERT INTO Hospital (hospital_pk, hospital_name,\
                            longitude, latitude, address, city, fips_code, zip)"
                                "VALUES ('{}', '{}', {}, {}, '{}', '{}', '{}', {})".format
                                    (row['hospital_pk'], row['hospital_name'],
                                    row['longitude'], row['latitude'],
                                    row['address'], row['city'],
                                    row['fips_code'], row['zip']))
            except Exception as e:
                try:
                    cur.execute("UPDATE Hospital"
                                    "SET hospital_name = '{0}',\
                                    longitude = {1},\
                                    latitude = {2},\
                                    address = '{3}',\
                                    city = '{4}',\
                                    fips_code = '{5}',\
                                    zip = '{6}'"
                                    "WHERE hospital_pk = '{7}'".format
                                    (row['hospital_name'], row['longitude'],
                                        row['latitude'], row['address'],
                                        row['city'], row['fips_code'],
                                        row['zip'], row['hospital_pk']))
                except Exception as e:
                    print("insert and update failed:", e)
                else:
                    num_rows_hospital_update += 1
            else:
                num_rows_hospital_insert += 1
    print("Info about", num_rows_hospital_insert, "hospitals are inserted.")
    print("Info about", num_rows_hospital_update, "hospitals are updated.")

    num_rows_beds = 0
    with conn.transaction():
        for index, row in df.iterrows():
            try:
                cur.execute("INSERT INTO Hospital_beds\
                        (all_adult_hospital_beds_7_day_avg,\
                        all_pediatric_inpatient_beds_7_day_avg,\
                        all_adult_hospital_inpatient_bed_occupied_7_day_coverage,\
                        all_pediatric_inpatient_bed_occupied_7_day_avg,\
                        total_icu_beds_7_day_avg,\
                        icu_beds_used_7_day_avg,\
                        inpatient_beds_used_covid_7_day_avg,\
                        staffed_adult_icu_patients_confirmed_covid_7_day_avg,\
                        hospital)"
                                "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, '{}')"\
                                .format(row['all_adult_hospital_beds_7_day_avg'],
                                row['all_pediatric_inpatient_beds_7_day_avg'],
                                row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'],
                                row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                                row['total_icu_beds_7_day_avg'],
                                row['icu_beds_used_7_day_avg'],
                                row['inpatient_beds_used_covid_7_day_avg'],
                                row['staffed_icu_adult_patients_confirmed_covid_7_day_avg'],
                                row['hospital_pk']))
            except Exception as e:
                print("insert failed:", e)
            else:
                num_rows_beds += 1
    print("Info about", num_rows_beds, "hospital beds are inserted.")

    conn.commit()
    conn.close()


# Load data
path_name = sys.argv[1]
df = pd.read_csv(path_name)
df = data_handle(df)
# SQL
run_sql(df[1:10])
