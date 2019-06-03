DROP TABLE IF EXISTS patients_cdph.main;
CREATE TABLE patients_cdph.main (
       entity_id	   SERIAL PRIMARY KEY,
       patient_id	   VARCHAR,
       source_id	   INT REFERENCES lookup_cdph.data_source (source_id),
       CONSTRAINT unique_combination UNIQUE (patient_id, source_id)
);


DROP TABLE IF EXISTS patients_cdph.names;
CREATE TABLE patients_cdph.names (
       id		   SERIAL PRIMARY KEY,
       entity_id	   INT REFERENCES patients_cdph.main (entity_id),
       update_date	   DATE,
       full_name	   VARCHAR,
       name_type	   name_type_enum DEFAULT 'birth'
);

DROP TABLE IF EXISTS patients_cdph.demographics;
CREATE TABLE patients_cdph.demographics (
       id		   SERIAL PRIMARY KEY,
       entity_id	   INT REFERENCES patients_cdph.main (entity_id),
       update_date	   DATE,
       date_of_birth	   DATE,
       date_of_death	   DATE,
       race_id		   INT REFERENCES lookup_cdph.race (race_id),
       ethnicity_id	   INT REFERENCES lookup_cdph.ethnicity (ethnicity_id),
       birth_address_id	   INT REFERENCES lookup_cdph.address (address_id),
       death_address_id	   INT REFERENCES lookup_cdph.address (address_id)
);
