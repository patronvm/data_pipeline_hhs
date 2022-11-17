This code will scrape data from Healthdata.gov and from
the center for medicare and medicaid services. Information such as
general hospital information, 7 day averages of hospital bed and occupancy
status, and an overall hospital rating are provided into 3 tables.
This is provided in the schema.sql file. In the load_hhs.py
file, the data will first be cleaned prior to inserting or updating new information.
The path of data to access is the first argument for this file.
In order to access the database, credentials in the credential.py must be updated
by the user. Two tables are updated/inserted with data in this py file, Hospital and
Hospital_beds. The load_quality.py file updates or inserts the overall quality
information of a hospital. To run this file, a date from when quality is updated
must be provided, along with where the data is coming from. Both files have try except
capabilities for the transactions, where if any error occurs at any stage, the insert/update
will not occur. 