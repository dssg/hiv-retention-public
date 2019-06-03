DROP TABLE IF EXISTS lookup_ucm.facilities;
CREATE TABLE lookup_ucm.facilities (
       facility_id	 SERIAL PRIMARY KEY,
       facility_name VARCHAR,
       facility_loc INT REFERENCES lookup_ucm.address (address_id),
       funding	 VARCHAR,
       facility_type VARCHAR
);

DROP TABLE IF EXISTS lookup_ucm.lab_types;
CREATE TABLE lookup_ucm.lab_types (
       lab_type_id	 SERIAL PRIMARY KEY,
       lab_type 	 VARCHAR
);

DROP TABLE IF EXISTS lookup_ucm.labs;
CREATE TABLE lookup_ucm.labs (
       lab_id	 SERIAL PRIMARY KEY,
       lab_name VARCHAR,
       lab_loc INT REFERENCES lookup_ucm.address (address_id),
       lab_type_id	 INT REFERENCES lookup_ucm.lab_types (lab_type_id),
       funding 		 VARCHAR
);

DROP TABLE IF EXISTS lookup_ucm.provider_name;
CREATE TABLE lookup_ucm.provider_name (
       provider_id	 SERIAL PRIMARY KEY,
       provider_name	 VARCHAR,
       gender 		 gender_enum,
       education	 VARCHAR
);

DROP TABLE IF EXISTS lookup_ucm.provider_info;
CREATE TABLE lookup_ucm.provider_info (
       provider_info_id	 	SERIAL PRIMARY KEY,
       provider_id	 	INT REFERENCES lookup_ucm.provider_name (provider_id),
       provider_dept	     	VARCHAR,
       source_id		INT REFERENCES lookup_ucm.data_source (source_id),
       provider_facility_id 	INT REFERENCES lookup_ucm.facilities (facility_id)
);
