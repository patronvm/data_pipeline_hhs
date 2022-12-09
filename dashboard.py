import streamlit as st
import pandas as pd
import psycopg
import credentials
import sys
import matplotlib.pyplot as plt
import plotly.graph_objects as go


# SQL for first analysis
report1 = "SELECT COUNT(*), collection_week FROM Hospital_beds \
GROUP BY collection_week \
ORDER BY collection_week DESC;"

# SQL for second analysis
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

# SQL for 3rd analysis
report3 = "SELECT used, total, (used / total) AS fraction, rating \
FROM \
(SELECT SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used, \
SUM(all_adult_hospital_beds_7_day_avg) + \
SUM(all_pediatric_inpatient_beds_7_day_avg) AS total, \
rating \
FROM \
(SELECT rating, day,\
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage, \
all_pediatric_inpatient_bed_occupied_7_day_avg,\
all_adult_hospital_beds_7_day_avg, \
all_pediatric_inpatient_beds_7_day_avg, collection_week \
FROM (SELECT rating, day, hospital FROM Rating \
    WHERE day = (SELECT MAX(day) FROM Rating)) AS R \
JOIN (SELECT all_adult_hospital_inpatient_bed_occupied_7_day_coverage, \
    all_pediatric_inpatient_bed_occupied_7_day_avg,\
    all_adult_hospital_beds_7_day_avg, \
    all_pediatric_inpatient_beds_7_day_avg, collection_week, hospital \
    FROM Hospital_beds \
    WHERE collection_week = (SELECT \
        MAX(collection_week) FROM Hospital_beds)) AS H \
ON R.hospital = H.hospital) AS X \
GROUP BY rating \
ORDER BY rating \
) AS Y;"

# SQL for 4th analysis
report4 = "SELECT \
    SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
    SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS all_used, \
    SUM(inpatient_beds_used_covid_7_day_avg) AS used_COVID, \
    collection_week \
    FROM Hospital_beds \
    GROUP BY collection_week \
    ORDER BY collection_week;"


# SQL for 5th analysis
report5 = "SELECT SUM(inpatient_beds_used_covid_7_day_avg) * 7, state \
FROM \
(SELECT state, inpatient_beds_used_covid_7_day_avg \
FROM (SELECT hospital_pk, state FROM Hospital) AS H \
JOIN (SELECT hospital, inpatient_beds_used_covid_7_day_avg \
    FROM Hospital_beds) AS B \
ON H.hospital_pk = B.hospital \
) AS X \
GROUP BY state;"

# SQL for 6th analysis
report6 = "SELECT hospital_name, city, state, changes \
FROM ((SELECT hospital_name, city, state, hospital_pk \
    FROM Hospital) AS H \
JOIN (SELECT ABS(new - old) AS changes, O.hospital \
    FROM (SELECT (inpatient_beds_used_covid_7_day_avg) AS new, \
        hospital FROM Hospital_beds \
        WHERE collection_week = (SELECT MAX(collection_week) \
                                 FROM Hospital_beds)) AS N \
        JOIN (SELECT (inpatient_beds_used_covid_7_day_avg) AS old, \
                hospital FROM Hospital_beds \
        WHERE collection_week = (SELECT MAX(collection_week) \
                                 FROM Hospital_beds \
            WHERE collection_week < (SELECT MAX(collection_week) \
                                     FROM Hospital_beds))) AS O \
        ON N.hospital = O.hospital) AS Y \
ON H.hospital_pk = Y.hospital) AS X \
WHERE changes IS NOT NULL \
ORDER BY changes DESC \
LIMIT 10 "

# SQL for 7th analysis
report7 = "SELECT (used/ (used + available)) AS utilization,\
                   state, collection_week \
FROM \
(SELECT (SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) + \
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)) AS used, \
(SUM(all_adult_hospital_beds_7_day_avg) + \
SUM(all_pediatric_inpatient_beds_7_day_avg)) AS available, \
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


# Runs postgresql with login info, uses
# sql code to create graphs and tables in dashboard

def run_sql():
    # Connects to the server
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()

    # Dataframe for the first analysis
    df1 = pd.DataFrame(columns=['Number of hospital records loaded',
                                'Load Week'])
    cur.execute(report1)

    # Takes inserted data from SQL and creates pandas dataframe
    for row in cur:
        load_num, load_week = row
        df0 = pd.DataFrame([[load_num, load_week]],
                           columns=['Number of hospital records loaded',
                                    'Load Week'])
        df1 = pd.concat([df1, df0], ignore_index=True)
    row = cur.fetchall()

    # DataFrame for second analysis, bed use
    df2 = pd.DataFrame(columns=['Week',
                                'Number of adult beds available',
                                'Number of pediatric beds available',
                                'Number used (adult)',
                                'Number used (pediatric)',
                                'Total number used',
                                'Number used by patients with COVID'])

    cur.execute(report2)

    # Placeholder variable names for loop to add data from SQL
    for row in cur:
        at, pt, au, pu, tu, cu, week = row
        df0 = pd.DataFrame([[week, at, pt, au, pu, tu, cu]],
                           columns=['Week', 'Number of adult beds available',
                                    'Number of pediatric beds available',
                                    'Number used (adult)',
                                    'Number used (pediatric)',
                                    'Total number used',
                                    'Number used by patients with COVID'])
        df2 = pd.concat([df2, df0], ignore_index=True)
    row = cur.fetchall()
    # Dataframe for used beds
    df2_used = df2[['Week', 'Number used (adult)',
                    'Number used (pediatric)',
                    'Total number used']]
    # Dataframe for available beds
    df2_available = df2[['Week',
                         'Number of adult beds available',
                         'Number of pediatric beds available']]
    # Dataframe for covid patients
    df2_covid = df2[['Week', 'Number used by patients with COVID']]

    # DF for analysis 3
    df3 = pd.DataFrame(columns=['Total number used',
                                'Total number available',
                                'Fraction',
                                'Rating'])

    cur.execute(report3)
    # SQL data to DF
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

    # DF for analysis 4
    df4 = pd.DataFrame(columns=['Number of hospital beds used (all cases)',
                                'Number of hospital beds used (COVID cases)',
                                'Week'])

    cur.execute(report4)
    # SQL data to DF
    for row in cur:
        all, covid, week = row
        df0 = pd.DataFrame(
            [[all, covid, week]],
            columns=['Number of hospital beds used (all cases)',
                     'Number of hospital beds used (COVID cases)',
                     'Week'])
        df4 = pd.concat([df4, df0], ignore_index=True)
    row = cur.fetchall()

    # DF for analysis 5
    df5 = pd.DataFrame(columns=['Number of COVID cases', 'State'])

    cur.execute(report5)
    # SQL data to dataframe
    for row in cur:
        case, state = row
        df0 = pd.DataFrame([[case, state]],
                           columns=['Number of COVID cases', 'State'])
        df5 = pd.concat([df5, df0], ignore_index=True)
    row = cur.fetchall()
    df5.dropna(inplace=True)

    # DF for analysis 6
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

    # DF for analysis 7
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
    # Returns the DF's necessary for dashboard
    return df1, df2_available, df2_used, df2_covid, df3, df4, df5, df6, df7


date = sys.argv[1]
df1, df2_available, df2_used, df2_covid, df3, df4, df5, df6, df7 = run_sql()


# Information about the updated hospital records in table format

def Q1():
    st.title("Updated Hospital Records")

    text = str(df1["Number of hospital records loaded"][0]) + " hospitals " +\
        "records were loaded in the most recent week (" + \
        str(df1["Load Week"][0]) + ") compared to " +\
        str(df1["Number of hospital records loaded"][1]) +\
        " hospitals from " +\
        str(df1["Load Week"][1])
    text
    st.dataframe(df1)


# Information about bed use split into 3 different tables

def Q2():
    st.title("Beds Used and Availability")

    "Table below summarizes beds availability for current week"
    ""
    st.dataframe(df2_available)

    "Table below summarizes beds used for current week"
    ""
    st.dataframe(df2_used)

    "Table below summarizes beds occupied by patients with COVID"
    ""
    st.dataframe(df2_covid)


# Plotting info of bed use by hospital quality with bar chart

def Q3():
    st.title("High Quality vs Low Quality Hospitals Beds Comparison")
    "The graph and table below summarizes the proportion of beds currently "
    "in use by hospital quality rating"
    fig_bar = plt.figure()
    plt.bar(df3["Rating"], df3["Fraction"])
    plt.title("Proportion of Beds Used vs Hospital Ratings")
    plt.xlabel('Rating')
    plt.ylabel('Fraction')
    st.pyplot(fig_bar)
    ""
    "Proportion of Beds Available by Hospital Rating"
    st.dataframe(df3)


# Line graph with individual points of bed use over time

def Q4():
    st.title("Bed Usage by Week Split by Case")
    "A plot of the total number of hospital beds used per week, "
    "over all time, split into all cases and COVID cases"
    fig = plt.figure()
    x = df4["Week"]
    y1 = df4["Number of hospital beds used (all cases)"]
    y2 = df4["Number of hospital beds used (COVID cases)"]
    # x, y2 = zip(*sorted(zip(x, y2)))
    plt.scatter(x, y1)
    plt.plot(x, y1, label="Number of hospital beds used (all cases)")
    plt.scatter(x, y2)
    plt.plot(x, y2, label="Number of hospital beds used (COVID cases)")
    plt.xticks(rotation=-30)
    plt.xlabel("Week")
    plt.ylabel("Beds Used")
    plt.title("Beds Used vs Time by Case")
    plt.legend(loc=7)
    st.pyplot(fig)


# US interactive map of covid cases with table for more info

def Q5():
    st.title("COVID-19 Cases by State")
    "A map showing the number of COVID cases by state"
    "From " + str(df4["Week"][0]) + " to " + str(df4["Week"].iloc[-1])
    fig = go.Figure(data=go.Choropleth(
        locations=df5["State"],
        z=df5["Number of COVID cases"].astype(float),
        locationmode="USA-states",
        colorscale="Blues",
        colorbar_title="Number of COVID cases"
    ))
    fig.update_layout(geo_scope="usa", title="COVID Cases by State")
    fig.update_layout(legend_title="Legend")
    st.plotly_chart(fig)
    # df4['Week'] = pd.to_datetime(df4['Week'])
    # df4 = df4.sort_values(by=['Week'])
    st.dataframe(df4)


# Table of the top 10 changes in covid cases

def Q6():
    st.title("Biggest Hospital Changes")
    "A table of the hospitals with the top 10 changes in COVID cases in "
    "the last week"
    st.dataframe(df6)


# Hospital utilization by state

def Q7():
    st.title("Hospital utilization graph")
    "The plot below shows the percent of available beds being used by "
    "state over time. Some states have missing data for some weeks."
    fig = plt.figure()
    state_list = []
    # for s in df7["State"]:
    #     if s not in state_list:
    #         state_list.append(s)
    # s = st.selectbox(label="State", options=state_list)
    for s in df7["State"]:
        if s not in state_list:
            state_list.append(s)
    s = st.multiselect("State", state_list)
    for ss in s:
        x = df7["Week"][df7["State"] == ss]
        y = df7["Hospital utilization"][df7["State"] == ss]
        x, y = zip(*sorted(zip(x, y)))
        # plt.xlim(min(x) - timedelta(days=3), max(x))
        # plt.ylim(min(y) - 0.03, max(y) + 0.03)
        plt.plot(x, y, '.-', label=ss)
        plt.xlabel("Week")
        plt.xticks(rotation=-30)
        plt.legend(loc="best")
    st.pyplot(fig)

    st.dataframe(df7)


# Layout of the dashboard for easy navigation
def layout():
    st.sidebar.write("Partridges")
    report_title = "Data Pipeline Report: " + date
    selectbox = st.sidebar.radio(report_title, ("Updated Hospital Records",
                                                "Beds Used and Availability",
                                                "High Quality vs Low Quality "
                                                "Hospitals Beds Comparison",
                                                "Bed Usage by Week "
                                                "Split by Case",
                                                "COVID-19 Cases by State",
                                                "Biggest Hospital Changes",
                                                "Hospital utilization graph"))
    if selectbox == "Updated Hospital Records":
        Q1()
    elif selectbox == "Beds Used and Availability":
        Q2()
    elif selectbox == "High Quality vs Low Quality Hospitals Beds Comparison":
        Q3()
    elif selectbox == "Bed Usage by Week Split by Case":
        Q4()
    elif selectbox == "COVID-19 Cases by State":
        Q5()
    elif selectbox == "Biggest Hospital Changes":
        Q6()
    elif selectbox == "Hospital utilization graph":
        Q7()


def main():
    layout()


if __name__ == "__main__":
    main()
