DROP TABLE IF EXISTS lookup_cdph.facilities;
CREATE TABLE lookup_cdph.facilities (
       facility_id	 VARCHAR PRIMARY KEY,
       facility_name VARCHAR,
       facility_loc INT REFERENCES lookup_cdph.address (address_id),
       funding	 VARCHAR,
       facility_type VARCHAR
);

DROP TABLE IF EXISTS lookup_cdph.lab_types;
CREATE TABLE lookup_cdph.lab_types (
       lab_type_id	 SERIAL PRIMARY KEY,
       lab_type 	 VARCHAR
);

DROP TABLE IF EXISTS lookup_cdph.labs;
CREATE TABLE lookup_cdph.labs (
       lab_id	 VARCHAR PRIMARY KEY,
       lab_name  VARCHAR,
       lab_loc 	 INT REFERENCES lookup_cdph.address (address_id),
       lab_type_id	 INT REFERENCES lookup_cdph.lab_types (lab_type_id),
       funding 		 VARCHAR
);

DROP TABLE IF EXISTS lookup_cdph.provider_name;
CREATE TABLE lookup_cdph.provider_name (
       provider_id	 VARCHAR PRIMARY KEY,
       provider_name	 VARCHAR,
       gender 		 gender_enum,
       education	 VARCHAR
);

DROP TABLE IF EXISTS lookup_cdph.provider_info;
CREATE TABLE lookup_cdph.provider_info (
       provider_info_id	 	SERIAL PRIMARY KEY,
       provider_id	 	VARCHAR REFERENCES lookup_cdph.provider_name (provider_id),
       provider_dept	     	VARCHAR,
       source_id		INT REFERENCES lookup_cdph.data_source (source_id),
       provider_facility_id 	VARCHAR REFERENCES lookup_cdph.facilities (facility_id)
);
