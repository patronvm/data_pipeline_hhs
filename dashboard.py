import streamlit as st
import pandas as pd
import psycopg
import credentials
import numpy as np
import sys
import matplotlib.pyplot as plt
import plotly.graph_objects as go

# 1
report1 = "SELECT COUNT(*), collection_week FROM Hospital_beds \
GROUP BY collection_week \
ORDER BY collection_week DESC;"

# 2
report2 = "SELECT SUM(all_adult_hospital_beds_7_day_avg) AS total_adult, \
SUM(all_pediatric_inpatient_beds_7_day_avg) AS total_pediatric, \
SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) AS used_adult, \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used_pediatric, \
SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) \
+ SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used, \
SUM(inpatient_beds_used_covid_7_day_avg) AS used_COVID, \
collection_week \
FROM Hospital_beds \
GROUP BY collection_week \
ORDER BY collection_week DESC \
LIMIT 5;"

# 3
report3 = "SELECT used, total, (used / total) AS fraction, rating \
FROM \
(SELECT SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used, \
SUM(all_adult_hospital_beds_7_day_avg) + \
SUM(all_pediatric_inpatient_beds_7_day_avg) AS total, \
rating \
FROM \
(SELECT rating, day, all_adult_hospital_inpatient_bed_occupied_7_day_coverage, \
all_pediatric_inpatient_bed_occupied_7_day_avg, all_adult_hospital_beds_7_day_avg, \
all_pediatric_inpatient_beds_7_day_avg, collection_week \
FROM (SELECT rating, day, hospital FROM Rating \
    WHERE day = (SELECT MAX(day) FROM Rating)) AS R \
JOIN (SELECT all_adult_hospital_inpatient_bed_occupied_7_day_coverage, \
    all_pediatric_inpatient_bed_occupied_7_day_avg, all_adult_hospital_beds_7_day_avg, \
    all_pediatric_inpatient_beds_7_day_avg, collection_week, hospital \
    FROM Hospital_beds \
    WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds)) AS H \
ON R.hospital = H.hospital) AS X \
GROUP BY rating \
ORDER BY rating \
) AS Y;"

# 4
report4 = "SELECT SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS all_used, \
SUM(inpatient_beds_used_covid_7_day_avg) AS used_COVID, \
collection_week \
FROM Hospital_beds \
GROUP BY collection_week;"


# 5
report5 = "SELECT SUM(inpatient_beds_used_covid_7_day_avg), state \
FROM \
(SELECT state, inpatient_beds_used_covid_7_day_avg \
FROM (SELECT hospital_pk, state FROM Hospital) AS H \
JOIN (SELECT hospital, inpatient_beds_used_covid_7_day_avg \
    FROM Hospital_beds) AS B \
ON H.hospital_pk = B.hospital \
) AS X \
GROUP BY state;"

# 6
report6 = "SELECT hospital_name, city, state, changes \
FROM ((SELECT hospital_name, city, state, hospital_pk \
    FROM Hospital) AS H \
JOIN (SELECT ABS(new - old) AS changes, O.hospital \
    FROM (SELECT (inpatient_beds_used_covid_7_day_avg) AS new, hospital FROM Hospital_beds \
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds)) AS N \
        JOIN (SELECT (inpatient_beds_used_covid_7_day_avg) AS old, hospital FROM Hospital_beds \
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds \
            WHERE collection_week < (SELECT MAX(collection_week) FROM Hospital_beds))) AS O \
        ON N.hospital = O.hospital) AS Y \
ON H.hospital_pk = Y.hospital) AS X \
WHERE changes IS NOT NULL \
ORDER BY changes DESC \
LIMIT 10 "

# 7
report7 = "SELECT (used/total) AS utilization, state, collection_week \
FROM \
(SELECT (SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)) AS used, \
(SUM(all_adult_hospital_beds_7_day_avg) + \
SUM(all_pediatric_inpatient_beds_7_day_avg)) AS total, \
state, collection_week \
FROM (SELECT all_adult_hospital_inpatient_bed_occupied_7_day_coverage, \
    all_pediatric_inpatient_bed_occupied_7_day_avg, \
    all_adult_hospital_beds_7_day_avg, \
    all_pediatric_inpatient_beds_7_day_avg, \
    collection_week, hospital \
    FROM Hospital_beds) AS B \
JOIN (SELECT hospital_pk, state FROM Hospital) AS H \
ON B.hospital = H.hospital_pk \
GROUP BY state, collection_week) AS X \
ORDER BY state;"



def run_sql():
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()

    # 1
    df1 = pd.DataFrame(columns=['Number of hospital records loaded', 'Load Week'])
    cur.execute(report1)
    for row in cur:
        load_num, load_week = row
        df0 = pd.DataFrame([[load_num, load_week]],
            columns=['Number of hospital records loaded', 'Load Week'])
        df1 = pd.concat([df1, df0], ignore_index=True)
    row = cur.fetchall()

    # 2
    df2 = pd.DataFrame(columns=['Week',
            'Number of adult beds available',
            'Number of pediatric beds available',
            'Number used (adult)',
            'Number used (pediatric)',
            'Total number used',
            'Number used by patients with COVID'])

    cur.execute(report2)
    for row in cur:
        at, pt, au, pu, tu, cu, week = row
        df0 = pd.DataFrame([[week, at, pt, au, pu, tu, cu]],
            columns=['Week', 'Number of adult beds available', \
            'Number of pediatric beds available', \
            'Number used (adult)',
            'Number used (pediatric)',
            'Total number used',
            'Number used by patients with COVID'])
        df2 = pd.concat([df2, df0], ignore_index=True)
    row = cur.fetchall()

    df2_used = df2[['Week', 'Number used (adult)',
            'Number used (pediatric)',
            'Total number used']]

    df2_available = df2[['Week',
            'Number of adult beds available',
            'Number of pediatric beds available']]

    df2_covid = df2[['Week','Number used by patients with COVID']]

    # 3
    df3 = pd.DataFrame(columns=['Total number used',
            'Total number available',
            'Fraction',
            'Rating'])

    cur.execute(report3)
    for row in cur:
        tu, tt, frac, rate = row
        df0 = pd.DataFrame([[tu, tt, frac, rate]],
            columns=['Total number used',
            'Total number available',
            'Fraction',
            'Rating'])
        df3 = pd.concat([df3, df0], ignore_index=True)
    row = cur.fetchall()
    df3.dropna(inplace=True)


    # 4
    df4 = pd.DataFrame(columns=['Number of hospital beds used (all cases)',
            'Number of hospital beds used (COVID cases)',
            'Week'])

    cur.execute(report4)
    for row in cur:
        all, covid, week = row
        df0 = pd.DataFrame([[all, covid, week]],
            columns=['Number of hospital beds used (all cases)',
            'Number of hospital beds used (COVID cases)',
            'Week'])
        df4 = pd.concat([df4, df0], ignore_index=True)
    row = cur.fetchall()

    # 5
    df5 = pd.DataFrame(columns=['Number of COVID cases',
            'State'])

    cur.execute(report5)
    for row in cur:
        case, state = row
        df0 = pd.DataFrame([[case, state]],
            columns=['Number of COVID cases',
            'State'])
        df5 = pd.concat([df5, df0], ignore_index=True)
    row = cur.fetchall()
    df5.dropna(inplace=True)

    # 6
    df6 = pd.DataFrame(columns=['Hospital name',
            'City',
            'State',
            'Changes in COVID cases in the last week'])

    cur.execute(report6)
    for row in cur:
        name, city, state, changes = row
        df0 = pd.DataFrame([[name, city, state, changes]],
            columns=['Hospital name',
            'City',
            'State',
            'Changes in COVID cases in the last week'])
        df6 = pd.concat([df6, df0], ignore_index=True)
    row = cur.fetchall()


    # 7
    df7 = pd.DataFrame(columns=['Hospital utilization',
            'State',
            'Week'])

    cur.execute(report7)
    for row in cur:
        u, state, week = row
        df0 = pd.DataFrame([[u, state, week]],
            columns=['Hospital utilization',
            'State',
            'Week'])
        df7 = pd.concat([df7, df0], ignore_index=True)
    row = cur.fetchall()
    df7.dropna(inplace=True)

    conn.commit()
    conn.close()

    return df1, df2_available, df2_used, df2_covid, df3, df4, df5, df6, df7

date = sys.argv[1]
df1, df2_available, df2_used, df2_covid, df3, df4, df5, df6, df7 = run_sql()

def Q1():
    st.title("Updated Hospital Records")

    text= str(df1["Number of hospital records loaded"][0]) + " hospitals " +\
    "records were loaded in the most recent week (" +str(df1["Load Week"][0])+\
    ") compared to " + str(df1["Number of hospital records loaded"][1]) + " hospitals from " +\
    str(df1["Load Week"][1])
    text
    st.dataframe(df1)

def Q2():
    st.title("Beds Use and Availability")

    "Table below summarizes beds availability for current week"
    ""
    st.dataframe(df2_available)

    "Table below summarizes beds used for current week"
    ""
    st.dataframe(df2_used)

    "Table below summarizes beds occupied by patients with COVID"
    ""
    st.dataframe(df2_covid)

def Q3():
    st.title("Analysis 3")
    "The graph and table below summarizes the proportion of beds currently in use by hospital quality rating"
    fig_bar = plt.figure()
    plt.bar(df3["Rating"], df3["Fraction"])
    plt.title("Proportion of Beds Used vs Hospital Ratings")
    plt.xlabel('Rating') 
    plt.ylabel('Fraction')
    st.pyplot(fig_bar)
    st.dataframe(df3)

def Q4():
    st.title("Analysis 4")
    "A plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases"
    fig = plt.figure()
    x = df4["Week"]
    y1 = df4["Number of hospital beds used (all cases)"]
    y2 = df4["Number of hospital beds used (COVID cases)"]
    plt.scatter(x, y1, label="Number of hospital beds used (all cases)")
    plt.scatter(x, y2, label="Number of hospital beds used (COVID cases)")
    plt.xticks(rotation=-30)
    plt.xlabel("Week")
    plt.legend(("Number of hospital beds used (all cases)",
        "Number of hospital beds used (COVID cases)"),
        loc=7)
    st.pyplot(fig)

    
def Q5():
    st.title("Analysis 5")
    "A map showing the number of COVID cases by state"
    fig = go.Figure(data=go.Choropleth(
        locations=df5["State"],
        z=df5["Number of COVID cases"].astype(float),
        locationmode="USA-states",
        colorscale="Blues"
    ))
    fig.update_layout(geo_scope="usa")
    st.plotly_chart(fig)


def Q6():
    st.title("Analysis 6")
    "A table of the hospitals with the top 10 changes in COVID cases in the last week"
    st.dataframe(df6)

def Q7():
    st.title("Analysis 7")
    "Graphs of hospital utilization (the percent of available beds being used) by state over time"
    st.dataframe(df7)
    fig = plt.figure()
    state_list = []
    for s in df7["State"]:
        if s not in state_list:
            x = df7["Week"][df7["State"]==s]
            y = df7["Hospital utilization"][df7["State"]==s]
            plt.scatter(x, y)
            state_list.append(s)
    plt.xlabel("Week")
    plt.xticks(rotation=-30)

    plt.legend(state_list, loc="best")
    st.pyplot(fig)



    

def layout():
    st.sidebar.write("Partridges")
    report_title = "Data Pipeline Report: " + date
    selectbox = st.sidebar.radio(report_title, ("Analysis 1", "Analysis 2",
    "Analysis 3", "Analysis 4", "Analysis 5", "Analysis 6",
    "Analysis 7"))
    if selectbox == "Analysis 1":
        Q1()
    elif selectbox == "Analysis 2":
        Q2()
    elif selectbox == "Analysis 3":
        Q3()
    elif selectbox == "Analysis 4":
        Q4()
    elif selectbox == "Analysis 5":
        Q5()
    elif selectbox == "Analysis 6":
        Q6()
    elif selectbox == "Analysis 7":
        Q7()

def main():
    layout()

if __name__ == "__main__":
    main()