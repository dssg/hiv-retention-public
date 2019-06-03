clean_ucm :
	psql -c 'DROP SCHEMA patients_ucm;'
	psql -c 'DROP SCHEMA lookup_ucm;'
	psql -c 'DROP SCHEMA events_ucm;'

create_ucm_tables : clean_ucm
	psql -f schema/create_schemas_ucm.sql
	psql -f schema/create_tables/01_create_simple_lookup_tables.sql
	psql -f schema/create_tables/02_create_provider_facility_lookup_tables.sql
	psql -f schema/create_tables/03_create_patients_tables.sql
	psql -f schema/create_tables/04_create_attribute_events_tables.sql
	psql -f schema/create_tables/05_create_visits_events_tables.sql

etl_ucm_patients:
        python load_ucm_common_schema.py '/home/aramachandran1/hiv_retention/hiv_retention/pipeline/luigi.yaml' patients

etl_ucm_events:
        python load_ucm_common_schema.py '/home/aramachandran1/hiv_retention/hiv_retention/pipeline/luigi.yaml' events

etl_ucm: create_ucm_tables etl_ucm_patients etl_ucm_events

crime:
	python load_crime_data.py

