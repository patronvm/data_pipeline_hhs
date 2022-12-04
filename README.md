This code will scrape data from Healthdata.gov and from
the center for medicare and medicaid services. Information such as
general hospital information, 7 day averages of hospital bed and occupancy
status, and an overall hospital rating are provided into 3 tables.


schema.sql:

This file will create 3 tables in sql with values to be stored from both websites.
The hospital table will contain general information about each hospital, such as
location and ownership, and can be updated to add new hospitals or updated general
information if necessary. The reference for this table is a hospital id number unique
to each hospital. The second table is a rating table for the hospital quality, and is expected
to be updated regularly by a given date. The third table is called Hospital_Beds, and will
contain 7 day averages of information such as bed status, employees, and occupancy. This
will reference the hospital id. 

load-hhs.py:

This file first cleans some general information from the data prior to
insertion and updating values, such as the default "NULL" as "-9999". It also
converts the geocoded address into longitude and latitude columns respectively.
This is done in the data handle function. In the run_sql function, sql commands
for inserting and updating information are provided. A csv file of invalid rows is also
created, for a user to update themselves before updating/inserting again.
The command line argument for this file is the data to insert and update. This file
has try except capabilites, and the transaction will work over the whole dataset.

load-quality.py:

This file is similarly structured to the load_hhs.py file, with a data handle,
and run sql functions. The command line arguement is also for the data for insertion. 


credentials.py:

This file is essential for the user to have their own username and password to
access the psql database without hardcoding this sensitive information into a document. 

Data:

This folder has data that is ready to be used for hospital information.


