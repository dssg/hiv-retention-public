
DROP TABLE IF EXISTS lookup_cdph.data_source;
CREATE TABLE lookup_cdph.data_source (
       source_id	 SERIAL PRIMARY KEY,
       source_name	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.race;
CREATE TABLE lookup_cdph.race (
       race_id	 SERIAL PRIMARY KEY,
       race	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.ethnicity;
CREATE TABLE lookup_cdph.ethnicity (
       ethnicity_id	 SERIAL PRIMARY KEY,
       ethnicity	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.address;
CREATE TABLE lookup_cdph.address (
       address_id	    SERIAL PRIMARY KEY,
       address_json	    TEXT -- JSON
);


DROP TABLE IF EXISTS lookup_cdph.address_type;
CREATE TABLE lookup_cdph.address_type (
       address_type_id	    SERIAL PRIMARY KEY,
       address_code         VARCHAR UNIQUE,
       address_type	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.icd9_codes;
CREATE TABLE lookup_cdph.icd9_codes (
       icd9_code	    VARCHAR PRIMARY KEY,
       icd9_text	    VARCHAR
);


DROP TABLE IF EXISTS lookup_cdph.icd10_codes;
CREATE TABLE lookup_cdph.icd10_codes (
      icd10_code	VARCHAR PRIMARY KEY,
      icd10_text	VARCHAR
);


DROP TABLE IF EXISTS lookup_cdph.status_codes;
CREATE TABLE lookup_cdph.status_codes (
       status_id	    SERIAL PRIMARY KEY,
       status	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.insurance;
CREATE TABLE lookup_cdph.insurance (
       insurance_id	    SERIAL PRIMARY KEY,
       insurance	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.treatment_types;
CREATE TABLE lookup_cdph.treatment_types (
       treatment_typ_id	    SERIAL PRIMARY KEY,
       treatment_typ	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_cdph.pharmacies;
CREATE TABLE lookup_cdph.pharmacies (
       pharmacy_id	    SERIAL PRIMARY KEY,
       name	    VARCHAR,
       pharm_loc INT REFERENCES lookup_cdph.address (address_id)
);


DROP TABLE IF EXISTS lookup_cdph.test_types;
CREATE TABLE lookup_cdph.test_types (
       test_type_id	    SERIAL PRIMARY KEY,
       test_type	    VARCHAR
);


DROP TABLE IF EXISTS lookup_cdph.encounter_types;
CREATE TABLE lookup_cdph.encounter_types (
       encounter_type_id	    SERIAL PRIMARY KEY,
       encounter_type	    VARCHAR
);
