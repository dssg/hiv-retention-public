DROP TABLE IF EXISTS lookup_ucm.data_source;
CREATE TABLE lookup_ucm.data_source (
       source_id	 SERIAL PRIMARY KEY,
       source_name	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.race;
CREATE TABLE lookup_ucm.race (
       race_id	 SERIAL PRIMARY KEY,
       race	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.ethnicity;
CREATE TABLE lookup_ucm.ethnicity (
       ethnicity_id	 SERIAL PRIMARY KEY,
       ethnicity	 VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.address;
CREATE TABLE lookup_ucm.address (
       address_id	    SERIAL PRIMARY KEY,
       address_json	    TEXT -- JSON
);


DROP TABLE IF EXISTS lookup_ucm.address_type;
CREATE TABLE lookup_ucm.address_type (
       address_type_id	    SERIAL PRIMARY KEY,
       address_type	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.icd9_codes;
CREATE TABLE lookup_ucm.icd9_codes (
       icd9_code	    VARCHAR PRIMARY KEY,
       icd9_text	    VARCHAR
);


DROP TABLE IF EXISTS lookup_ucm.icd10_codes;
CREATE TABLE lookup_ucm.icd10_codes (
      icd10_code	VARCHAR PRIMARY KEY,
      icd10_text	VARCHAR
);

DROP TABLE IF EXISTS lookup_ucm.icd9_info;
CREATE TABLE lookup_ucm.icd9_info (
       icd9_code            VARCHAR REFERENCES lookup_ucm.icd9_codes(icd9_code),
       sti		    INT,
       oi		    INT,
       psychiatric_illness  INT,
       pregnancy_related_dx INT,
       substance_use	    INT
);

DROP TABLE IF EXISTS lookup_ucm.icd10_info;
CREATE TABLE lookup_ucm.icd10_info (
       icd10_code            VARCHAR REFERENCES lookup_ucm.icd10_codes(icd10_code),
       sti		    INT,
       oi		    INT,
       psychiatric_illness  INT,
       pregnancy_related_dx INT,
       substance_use        INT
);

DROP TABLE IF EXISTS lookup_ucm.status_codes;
CREATE TABLE lookup_ucm.status_codes (
       status_id	    SERIAL PRIMARY KEY,
       status	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.insurance;
CREATE TABLE lookup_ucm.insurance (
       insurance_id	    SERIAL PRIMARY KEY,
       insurance	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.treatment_types;
CREATE TABLE lookup_ucm.treatment_types (
       treatment_typ_id	    SERIAL PRIMARY KEY,
       treatment_typ	    VARCHAR UNIQUE
);


DROP TABLE IF EXISTS lookup_ucm.pharmacies;
CREATE TABLE lookup_ucm.pharmacies (
       pharmacy_id	    SERIAL PRIMARY KEY,
       name	    VARCHAR,
       pharm_loc INT REFERENCES lookup_ucm.address (address_id)
);


DROP TABLE IF EXISTS lookup_ucm.test_types;
CREATE TABLE lookup_ucm.test_types (
       test_type_id	    SERIAL PRIMARY KEY,
       test_type	    VARCHAR,
       procedure_type	    VARCHAR,
       component_type	    VARCHAR
);


DROP TABLE IF EXISTS lookup_ucm.encounter_types;
CREATE TABLE lookup_ucm.encounter_types (
       encounter_type_id	    SERIAL PRIMARY KEY,
       encounter_type	    VARCHAR
);
