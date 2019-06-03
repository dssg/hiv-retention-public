DROP TABLE IF EXISTS patients_ucm.main;
CREATE TABLE patients_ucm.main (
       entity_id	   SERIAL PRIMARY KEY,
       patient_id	   VARCHAR,
       source_id	   INT REFERENCES lookup_ucm.data_source (source_id),
       CONSTRAINT unique_combination UNIQUE (patient_id, source_id)
);


DROP TABLE IF EXISTS patients_ucm.names;
CREATE TABLE patients_ucm.names (
       id		   SERIAL PRIMARY KEY,
       entity_id	   INT REFERENCES patients_ucm.main (entity_id),
       update_date	   DATE,
       full_name	   VARCHAR,
       name_type	   name_type_enum DEFAULT 'birth',
       CONSTRAINT unique_name_combination UNIQUE (entity_id, update_date, full_name, name_type)
);

DROP TABLE IF EXISTS patients_ucm.demographics;
CREATE TABLE patients_ucm.demographics (
       id		   SERIAL PRIMARY KEY,
       entity_id	   INT REFERENCES patients_ucm.main (entity_id),
       update_date	   DATE,
       date_of_birth	   DATE,
       date_of_death	   DATE,
       race_id		   INT REFERENCES lookup_ucm.race (race_id),
       ethnicity_id	   INT REFERENCES lookup_ucm.ethnicity (ethnicity_id),
       birth_address_id	   INT REFERENCES lookup_ucm.address (address_id),
       death_address_id	   INT REFERENCES lookup_ucm.address (address_id)
);
