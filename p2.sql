BEGIN;
insert into Hospital (hospital_pk, hospital_name, type, ownership, emergency_services, longitude, latitude, address, city, fips_code, county_name, state, zip)
values (hospital_pk, hospital_name, hospital_subtype, Hospital Ownership, Emergency Services, Longitude, Latitude, Address, City, fips_code, County Name, State, ZIP Code);
COMMIT;

BEGIN;
insert into Rating (day, rating, hospital)
values (quality_date, Hospital Overall Rating, Facility ID);
COMMIT;


BEGIN;
insert into Hospital_beds (all_adult_hospital_beds_7_day_avg, 
	all_pediatric_inpatient_beds_7_day_avg,
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
	all_pediatric_inpatient_bed_occupied_7_day_avg,
	total_icu_beds_7_day_avg,
	icu_beds_used_7_day_avg,
	inpatient_beds_used_covid_7_day_avg,
	staffed_adult_icu_patients_confirmed_covid_7_day_avg)
values (all_adult_hospital_beds_7_day_avg, 
	all_pediatric_inpatient_beds_7_day_avg,
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
	all_pediatric_inpatient_bed_occupied_7_day_avg,
	total_icu_beds_7_day_avg,
	icu_beds_used_7_day_avg,
	inpatient_beds_used_covid_7_day_avg,
	staffed_adult_icu_patients_confirmed_covid_7_day_avg);
COMMIT;

