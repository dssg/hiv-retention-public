DROP TABLE IF EXISTS events_cdph.events;
CREATE TABLE events_cdph.events (
       event_id			SERIAL PRIMARY KEY,
       event_type		VARCHAR,
       entity_id   		INT REFERENCES patients_cdph.main (entity_id),
       update_date     	    	DATE,
       batch_id			VARCHAR, -- billing id for cdph
       source_id           	INT REFERENCES lookup_cdph.data_source (source_id),
       insurance_id 	    	INT REFERENCES lookup_cdph.insurance (insurance_id),
       -- visit related
       visit_date		DATE,
       visit_status_id  	INT REFERENCES lookup_cdph.status_codes (status_id),
       visit_notes 		TEXT,
       provider_id 	    	VARCHAR REFERENCES lookup_cdph.provider_name (provider_id),
       facility_id 	   	VARCHAR REFERENCES lookup_cdph.facilities (facility_id),
       id_provider_flag     	INT,
       encounter_type_id    	INT REFERENCES lookup_cdph.encounter_types (encounter_type_id),
       -- diagnosis related
       icd9_code  	    	VARCHAR REFERENCES lookup_cdph.icd9_codes (icd9_code),
       icd10_code  	    	VARCHAR REFERENCES lookup_cdph.icd10_codes (icd10_code),
       diagnosis		VARCHAR,
       dx_rank  	    	INT,
       pregnancy_related_dx 	INT,
       opportunistic_infection  INT,
       sti  			INT,
       psychiatric_illness  	INT,
       substance_use 		INT,
       -- treatment related
       treatment_start_date	DATE,
       treatment_end_date     	DATE,
       treatment_name 		VARCHAR,
       treatment_type_id     	INT REFERENCES lookup_cdph.treatment_types (treatment_typ_id),
       treatment_frequency 	VARCHAR,
       treatment_dose 		VARCHAR,
       treatment_notes 		TEXT,
       art			INT, -- binary
       psychiatric_medication	INT, -- binary
       opioid			INT, -- binary
       oi_prophylaxis		INT, -- binary
       pharmacy_id 	    	INT REFERENCES lookup_cdph.pharmacies (pharmacy_id),
       -- lab related
       lab_order_date		DATE,
       lab_collection_date      DATE,
       lab_result_date  	DATE,
       lab_test_type_cd 	VARCHAR REFERENCES lookup_cdph.lab_test_type (code),
       lab_test_value 	    	VARCHAR,
       lab_test_result_descr    VARCHAR,
       lab_anonymous 	    	INT,
       lab_id 		    	VARCHAR REFERENCES lookup_cdph.labs (lab_id)
);
