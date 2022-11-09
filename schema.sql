/* We created tables based on properties, location, and 
the number of beds in the hospitals
*/

/* The Hospital table contains inforamtion about general 
properties of hospital. We set the unique ID for each hospital 
as primary key. Attributes in the entity are related but
independent to each other.
*/

/* The Location table contains inforamtion about the location
of hospital. The table have hospital_id attribute references 
the primary key of Hospital table, where we can join them in
order to reduce redundancy. Notice that though city, fips_code,
state, and zip attributes are dependent, it is convenient to
have them in a single table without a join.
*/

CREATE TABLE Hospital (
	hospital_pk VARCHAR PRIMARY KEY,
	hospital_name VARCHAR, 
	type VARCHAR,
	ownership VARCHAR,
	emergency_services BOOLEAN,
	longitude FLOAT CHECK (longitude > -180 AND longitude < 180),
	latitude FLOAT CHECK (latitude > -90 AND latitude < 90),
	address VARCHAR,
	city VARCHAR,
	fips_code VARCHAR,
	county_name VARCHAR,
	state VARCHAR,
	zip INTEGER	
);

CREATE TABLE Rating (
	day DATE,
	rating INTEGER CHECK (rating > 0),
	hospital VARCHAR REFERENCES Hospital
);


CREATE TABLE Hospital_beds (
	all_adult_hospital_beds_7_day_avg FLOAT, 
	all_pediatric_inpatient_beds_7_day_avg FLOAT,
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage FLOAT,
	all_pediatric_inpatient_bed_occupied_7_day_avg FLOAT,
	total_icu_beds_7_day_avg FLOAT,
	icu_beds_used_7_day_avg FLOAT,
	inpatient_beds_used_covid_7_day_avg FLOAT,
	staffed_adult_icu_patients_confirmed_covid_7_day_avg FLOAT,
	hospital VARCHAR REFERENCES Hospital
);

