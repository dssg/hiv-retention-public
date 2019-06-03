clean_cdph :
	psql -c 'DROP SCHEMA patients_cdph;'
	psql -c 'DROP SCHEMA lookup_cdph;'
	psql -c 'DROP SCHEMA events_cdph;'

create_cdph_tables : clean_cdph
        psql -f schema/create_schemas_cdph.sql
        psql -f	schema/create_tables/01_create_simple_lookup_tables_cdph.sql
        psql -f	schema/create_tables/02_create_provider_facility_lookup_tables_cdph.sql
        psql -f	schema/create_tables/03_create_patients_tables_cdph.sql
        psql -f	schema/create_tables/04_create_attribute_events_tables_cdph.sql
        psql -f	schema/create_tables/05_create_visits_events_tables_cdph.sql

etl_cdph_patients:
	python load_cdph_common_schema.py '/home/aramachandran1/hiv_retention/hiv_retention/pipeline/luigi.yaml' patients

etl_cdph_events:
	python load_cdph_common_schema.py '/home/aramachandran1/hiv_retention/hiv_retention/pipeline/luigi.yaml' events

etl_cdph: create_cdph_tables etl_cdph_patients etl_cdph_events

load_raw_cdph :
	python load_raw_cdph.py '/home/aramachandran1/hiv_retention/hiv_retention/pipeline/luigi.yaml'
