/*Q1*/

/*A summary of how many hospital records were loaded in the most recent week*/
SELECT COUNT(*), collection_week FROM Hospital_beds
GROUP BY collection_week
ORDER BY collection_week DESC;

/*Q2*/
/*the number of adult and pediatric beds available this week*/
SELECT SUM(all_adult_hospital_beds_7_day_avg) AS total_adult,
SUM(all_pediatric_inpatient_beds_7_day_avg) AS total_pediatric,
/*the number used, and the number used by patients with COVID*/
SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) AS used_adult,
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used_pediatric,
SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)
+ SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used,
SUM(inpatient_beds_used_covid_7_day_avg) AS used_COVID,
collection_week
FROM Hospital_beds
GROUP BY collection_week
ORDER BY collection_week DESC;
/*LIMIT 5*/

/*Q3*/

/*the fraction of beds currently in use by hospital quality rating*/

SELECT used, total, (used / total) AS fraction, rating
FROM
(SELECT SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) +
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS used, 
SUM(all_adult_hospital_beds_7_day_avg) +
SUM(all_pediatric_inpatient_beds_7_day_avg) AS total,
rating
FROM
(SELECT rating, day, all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
all_pediatric_inpatient_bed_occupied_7_day_avg, all_adult_hospital_beds_7_day_avg,
all_pediatric_inpatient_beds_7_day_avg, collection_week
FROM (SELECT rating, day, hospital FROM Rating
    WHERE day = (SELECT MAX(day) FROM Rating)) AS R
JOIN (SELECT all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
    all_pediatric_inpatient_bed_occupied_7_day_avg, all_adult_hospital_beds_7_day_avg,
    all_pediatric_inpatient_beds_7_day_avg, collection_week, hospital
    FROM Hospital_beds
    WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds)) AS H
ON R.hospital = H.hospital) AS X
GROUP BY rating
ORDER BY rating
) AS Y;

/*Q4*/

SELECT SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) +
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS all_used,
SUM(inpatient_beds_used_covid_7_day_avg) AS used_COVID,
collection_week
FROM Hospital_beds
GROUP BY collection_week;

/*Q5*/
/*A map showing the number of COVID cases by state (the first two digits of a hospital ZIP code is its state)*/
SELECT SUM(inpatient_beds_used_covid_7_day_avg), state
FROM
(SELECT state, inpatient_beds_used_covid_7_day_avg
FROM (SELECT hospital_pk, state FROM Hospital) AS H
JOIN (SELECT hospital, inpatient_beds_used_covid_7_day_avg
    FROM Hospital_beds) AS B
ON H.hospital_pk = B.hospital
) AS X
GROUP BY state;

/*Q6*/
/*A table of the hospitals (including names and locations)
with the largest changes in COVID cases in the last week*/
SELECT hospital_name, longitude, latitude, address, city, state, changes
FROM
((SELECT hospital_name, longitude, latitude, address, city, state, hospital_pk
    FROM Hospital) AS H
JOIN (SELECT ABS(new - old) AS changes, O.hospital
    FROM (SELECT (inpatient_beds_used_covid_7_day_avg) AS new, hospital FROM Hospital_beds
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds)) AS N
        JOIN (SELECT (inpatient_beds_used_covid_7_day_avg) AS old, hospital FROM Hospital_beds
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds
            WHERE collection_week < (SELECT MAX(collection_week) FROM Hospital_beds))
            ) AS O
        ON N.hospital = O.hospital
            ) AS Y
ON H.hospital_pk = Y.hospital) AS X
WHERE changes = (SELECT MAX(changes) FROM
    ((SELECT hospital_name, longitude, latitude, address, city, state, hospital_pk
    FROM Hospital) AS H
JOIN (SELECT ABS(new - old) AS changes, O.hospital
    FROM (SELECT (inpatient_beds_used_covid_7_day_avg) AS new, hospital FROM Hospital_beds
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds)) AS N
        JOIN (SELECT (inpatient_beds_used_covid_7_day_avg) AS old, hospital FROM Hospital_beds
        WHERE collection_week = (SELECT MAX(collection_week) FROM Hospital_beds
            WHERE collection_week < (SELECT MAX(collection_week) FROM Hospital_beds))
            ) AS O
        ON N.hospital = O.hospital
            ) AS Y
ON H.hospital_pk = Y.hospital) AS X);


/*Q7*/
/*Graphs of hospital utilization (the percent of available beds being used) by state,
or by type of hospital (private or public), over time*/
SELECT (used/total) AS utilization, state, collection_week
FROM
(SELECT (SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage) +
SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)) AS used,
(SUM(all_adult_hospital_beds_7_day_avg) +
SUM(all_pediatric_inpatient_beds_7_day_avg)) AS total,
state, collection_week
FROM (SELECT all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
    all_pediatric_inpatient_bed_occupied_7_day_avg,
    all_adult_hospital_beds_7_day_avg,
    all_pediatric_inpatient_beds_7_day_avg,
    collection_week, hospital
    FROM Hospital_beds) AS B
JOIN (SELECT hospital_pk, state FROM Hospital) AS H
ON B.hospital = H.hospital_pk
GROUP BY state, collection_week) AS X
ORDER BY state;