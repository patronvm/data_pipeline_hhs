/* We created tables based on the entities of the properties and location,
rating, and the number of beds in the hospitals. We chose these entities
based on what information being updated and grouping relevant information
together.
*/


/* The Hospital table contains inforamtion about general 
properties of hospital. We set the unique ID for each hospital 
as primary key. The table include location information. 
Notice that though city, fips_code, state, and zip attributes 
are dependent, it is convenient to have those attributes in a
single table without a join.
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

/* The Rating table contains hospital ratings that update multiple
times a year. These update will be recorded in this table.
We assign continuous id as primary key to identify a specific version
of rating. We can reference the Hospital table with the foreign key
*/
CREATE TABLE Rating (
	id SERIAL PRIMARY KEY,
	day DATE,
	rating INTEGER CHECK (rating > 0),
	hospital VARCHAR REFERENCES Hospital
);

/*The Hospital_beds table contains information about hospital bed
status. They are updated with weekly information*/
CREATE TABLE Hospital_beds (
	collection_week DATE,
	all_adult_hospital_beds_7_day_avg FLOAT, 
	all_pediatric_inpatient_beds_7_day_avg FLOAT,
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage FLOAT,
	all_pediatric_inpatient_bed_occupied_7_day_avg FLOAT,
	total_icu_beds_7_day_avg FLOAT,
	icu_beds_used_7_day_avg FLOAT,
	inpatient_beds_used_covid_7_day_avg FLOAT,
	staffed_adult_icu_patients_confirmed_covid_7_day_avg FLOAT,
	hospital VARCHAR REFERENCES Hospital,
	PRIMARY KEY (hospital, collection_week)
);

