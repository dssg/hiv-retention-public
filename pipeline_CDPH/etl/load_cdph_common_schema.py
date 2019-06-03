import sys
import yaml

from sqlalchemy.sql import text
from sqlalchemy import create_engine


def get_db_conn(postgres_config):
    """Get db connection (bad code)."""
    with open(postgres_config, 'r') as f:
        config = yaml.load(f)
    dbtype = 'postgres'

    # previously was: user = config['postgres']['user']
    user = config['user']
    host = config['host']
    port = config['port']
    db = config['database']
    passcode = config['password']
    url = '{}://{}:{}@{}:{}/{}'.format(dbtype,
                                       user,
                                       passcode,
                                       host,
                                       port,
                                       db)
    conn = create_engine(url)
    return conn

def load_patient_data(connection, source_suffix):
    """Load data about patients into the common schema. 
    In:
        - connection: connection to the database
    """
    query = text('''
            with patients as (
                    SELECT "EHARSID" as patient_id
                    FROM raw_cdph.person
                    )

            INSERT INTO {target_table}.main (patient_id, source_id)
                    SELECT patient_id, :source_id from patients
            ON CONFLICT("patient_id","source_id") DO NOTHING;
            '''.format(**{'target_table': 'patients_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query, source_id=source_id)
    trans.commit()
    trans.close()

    # patient demographics
    # keep only the high level option and manually add the mixed race option
    query = text('''
            WITH races AS ( 
                SELECT distinct "Description" as race_desc
                FROM raw_cdph.race
                WHERE char_length("Code") < 5
                )
            INSERT INTO {target_table}.race (race)
                SELECT race_desc FROM races
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()
    query = text('''
            INSERT INTO {target_table}.race (race)
                VALUES ('Mixed')
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    connection.execute(query)
    trans.commit()
    trans.close()

    query = text('''
            WITH ethnicities AS ( 
                SELECT distinct "Description" as ethnicity_desc
                FROM raw_cdph.ethnicity
                WHERE char_length("Code") < 5
                )
            INSERT INTO {target_table}.ethnicity (ethnicity)
                SELECT ethnicity_desc FROM ethnicities
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    query = text('''
                with patients as (
                    select entity_id,
                        race1 as race_code,
                        case when race2 is null
                            then raw_cdph.race."Description" 
                            else 'Mixed' 
                            end as race_discription ,
                        ethnicity_id, ethnicity,
                        race_id as race_id, 
                        date '1960-01-01' + "DOB"::int as dob,
                        date '1960-01-01' + "DOD"::int as dod
                    from raw_cdph.person
                    join patients_cdph.main 
                        on "EHARSID"=patient_id
                    join raw_cdph.race
                        on raw_cdph.race."Code" = race1
                    join lookup_cdph.race
                        on case when race2 is null
                            then raw_cdph.race."Description" 
                            else 'Mixed' 
                            end = lookup_cdph.race.race
                    join raw_cdph.ethnicity
                        on raw_cdph.ethnicity."Code" = ethnicity1
                    join lookup_cdph.ethnicity
                        on raw_cdph.ethnicity."Description" = ethnicity
                    )
                INSERT INTO {target_table}.demographics 
                        (entity_id, date_of_birth, date_of_death, race_id, ethnicity_id)
                    SELECT entity_id, dob, dod, race_id, ethnicity_id from patients
                ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'patients_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

def load_facility_data(connection, source_suffix):
    """Load data about facilities into the common schema.
    In:
        - connection: connection to the database
    """
    # none of the CDPH facilities have funding so ignoring for now
    # need to still add addresses
    query = text('''
            WITH facilities AS ( 
                SELECT 
                    concat_ws(' ', name1::text, name2::text) as name,
                    "Description" as facility_type,
                    "FACILITYID" as facility_id
                FROM raw_cdph.facility_code
                LEFT JOIN raw_cdph.facility_type
                    ON "Code" = facility_type_cd
                
                )
            INSERT INTO {target_table}.facilities (facility_id, facility_name, facility_type)
                SELECT facility_id, name,facility_type FROM facilities
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

def load_event_data(connection, source_suffix):
    """Load data about events (labs, infections, gender, address)into the common schema. 
    In:
        - connection: connection to the database
    """
    query = text('''
            WITH lab_types AS ( 
                SELECT distinct "Description" as lab_desc
                FROM raw_cdph.lab_type
                )
            INSERT INTO {target_table}.lab_types (lab_type)
                SELECT lab_desc FROM lab_types
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    _ = connection.execute(query)

    query = text('''
            WITH labs AS ( 
                SELECT
                    clia_uid,
                    concat_ws(' ', lab_name1::text, lab_name2::text) as name,
                    lab_type_id as lab_type_id
                FROM raw_cdph.clia_code
                LEFT JOIN raw_cdph.lab_type
                    ON raw_cdph.lab_type."Code"::int = raw_cdph.clia_code.lab_type::int
                LEFT JOIN lookup_cdph.lab_types
                    ON raw_cdph.lab_type."Description" = lookup_cdph.lab_types.lab_type
                )
            INSERT INTO {target_table}.labs (lab_id,lab_name, lab_type_id)
                SELECT clia_uid, name, lab_type_id FROM labs
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # populate provider info
    # speciality code doesn't exist in this data; might later need to form a lookup for this
    query = text('''
            WITH providers AS ( 
                SELECT 
                    json_build_object(
                        'name_prefix', name_prefix, 
                        'first_name', first_name, 
                        'middle_name', middle_name, 
                        'last_name', last_name,
                        'name_suffix', name_suffix)
                        as full_name,
                    specialty_cd,
                    "PROVIDERID" as provider_id
                FROM raw_cdph.provider_code
                ),
            new_providers AS (
                    INSERT INTO {target_table}.provider_name (provider_id, provider_name)
                        SELECT provider_id, full_name FROM providers
                    ON CONFLICT DO NOTHING
                    RETURNING provider_id, provider_name
                )
            INSERT INTO {target_table}.provider_info (provider_id, provider_dept, source_id)
                SELECT new_providers.provider_id, specialty_cd, :source_id
                FROM new_providers
                JOIN providers 
                    ON provider_name::text = full_name::text
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query,  source_id=source_id)
    trans.commit()
    trans.close()

    # need to add an event key with every addition to any events table
    # map to the gender enums
    query = text('''
            WITH genders AS ( 
                SELECT 
                    CASE
                        WHEN current_gender = 'MF' THEN 'transgender female'
                        WHEN current_gender = 'FM' THEN 'transgender male'
                        WHEN birth_sex = 'M' THEN 'male'
                        WHEN birth_sex = 'F' THEN 'female'
                        WHEN birth_sex IS NULL THEN NULL
                        ELSE 'other'
                    END as gender_id,
                    date '1960-01-01' + "HIV_DX_DT"::int as earliest_date,
                    patient_id,
                    entity_id
                FROM raw_cdph.person
                JOIN patients_cdph.main
                    ON patient_id = "EHARSID"
                ),
            new_event AS (
                    INSERT INTO {target_table}.events (event_type, entity_id, update_date)
                        SELECT 'gender' as event_type, 
                            entity_id,  
                            earliest_date as update_date
                        FROM genders
                    ON CONFLICT DO NOTHING
                    RETURNING event_id, entity_id
                )
            INSERT INTO {target_table}.gender (event_id, gender_id)
                SELECT event_id, gender_id::gender_enum FROM genders
                JOIN new_event 
                ON genders.entity_id = new_event.entity_id
            ON CONFLICT DO NOTHING;
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # Loading addresses
    query = text('''
            WITH addresses AS ( 
                SELECT entity_id, 
                    address_seq, address_type_cd, address_type_id,
                    zip_cd,
                    date '1960-01-01' + "ENTER_DT"::int as update_date
                FROM raw_cdph.address
                JOIN raw_cdph.document USING ("DOCUMENTID")
                JOIN patients_cdph.main
                    ON patient_id = "EHARSID"
                JOIN lookup_cdph.address_type
                    ON address_type_cd = address_code
                ),
            new_event AS (
                    INSERT INTO {target_table}.events (event_type, entity_id, update_date)
                        SELECT 'address' as event_type, 
                            entity_id,  
                            update_date as update_date
                        FROM addresses
                    ON CONFLICT DO NOTHING
                    RETURNING event_id, entity_id
                )
            INSERT INTO {target_table}.address (event_id, address_type_id, address_json, address_seq)
                SELECT event_id, address_type_id, 
                    json_build_object('zipcode', zip_cd) as address,
                    address_seq
                FROM addresses
                JOIN new_event 
                ON addresses.entity_id = new_event.entity_id
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # combining the labtests by concatenating
    query = text('''
            CREATE TABLE lookup_cdph.lab_test_type
            (
                code   VARCHAR PRIMARY KEY,
                description VARCHAR
            )
            ;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    connection.execute(query)
    query = text('''
            INSERT INTO lookup_cdph.lab_test_type (code, description)
            SELECT 
                raw_cdph.lab_test_code.code || ' ' || raw_cdph.lab_vl_test_type.code as code,
                raw_cdph.lab_test_code.description || ' ' || raw_cdph.lab_vl_test_type.description as description
            FROM raw_cdph.lab_test_code 
                CROSS JOIN raw_cdph.lab_vl_test_type
            UNION SELECT code, description FROM raw_cdph.lab_test_code;
            ;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    connection.execute(query)

    # add speciman type?
    query = text('''
            CREATE TEMP TABLE labs AS  
                SELECT 
                    entity_id, 
                    "RESULT_RPT_DT" as update_date,
                    "DOCUMENTID" as batch_id,
                    lab_test_cd || 
                        CASE WHEN lab_test_type IS NULL
                            THEN ''
                            ELSE ' ' || lab_test_type::int
                            END
                        as lab_test_type_cd, 
                    result || 
                        CASE WHEN raw_cdph.lab_result_units.description IS NULL
                            THEN ''
                            ELSE ' ' || raw_cdph.lab_result_units.description 
                            END
                        as lab_test_value, 
                    result_interpretation as lab_test_result_descr,
                    lab_id,
                    raw_cdph.lab_with_dates."PROVIDERID" as provider_id,
                    raw_cdph.lab_with_dates."FACILITYID" as facility_id
                FROM raw_cdph.lab_with_dates
                LEFT JOIN raw_cdph.document USING ("DOCUMENTID")
                LEFT JOIN lookup_cdph.labs 
                    ON lab_id = clia_uid
                LEFT JOIN raw_cdph.lab_result_units 
                    ON result_units = code
                JOIN patients_cdph.main
                    ON patient_id = "EHARSID";
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # add an event for each lab
    query = text('''
            INSERT INTO {target_table}.events (entity_id, update_date, batch_id, source_id,
                    lab_test_type_cd, lab_test_value, lab_test_result_descr,
                    lab_id, provider_id, facility_id, event_type )
                SELECT entity_id, 
                    update_date as update_date, 
                    batch_id, 
                    1 as source_id,
                    lab_test_type_cd, lab_test_value, lab_test_result_descr,
                    lab_id, provider_id, facility_id, 'lab'
                FROM labs
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # diagnoses are from OI? nothing else there really
    # also add HIV? but what exactly?
    query = text('''
            WITH diagnoses AS (
                SELECT 
                    entity_id,      
                    description as diagnosis,
                    date '1960-01-01' + "DX_DT"::int as visit_date,
                    date '1960-01-01' + "ENTER_DT"::int as update_date,
                    "DOCUMENTID" as batch_id,
                    "PROVIDERID" as provider_id,
                    "FACILITYID" as facility_id
                FROM raw_cdph.oi
                LEFT JOIN raw_cdph.document USING ("DOCUMENTID")
                LEFT JOIN raw_cdph.oi_diagnosis_cd
                    ON code = oi_cd
                JOIN patients_cdph.main
                    ON patient_id = "EHARSID"
                )
                INSERT INTO {target_table}.events (entity_id, update_date, batch_id, source_id,
                    visit_date, diagnosis, opportunistic_infection,
                    provider_id, facility_id, event_type )
                SELECT entity_id, update_date, batch_id, 
                    1 as source_id,
                    visit_date, diagnosis, 
                    1 as opportunistic_infection,
                    provider_id, facility_id, 'visit' 
                FROM diagnoses
                ;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    
def load_cdph_common_schema(postgres_config, load_patients, load_events):
    """Load data from CDPH into the common schema.
    In:
        - postgres_config: path to postgres_config
    """
    engine = get_db_conn(postgres_config)
    connection = engine.connect()

    # We start with the source_id.
    source_suffix = 'cdph'
    source_name='chicago_department_of_public_health'

    query = text('''
            INSERT INTO {source_table}.data_source (source_name) 
                VALUES (:source_name) 
            ON CONFLICT("source_name") DO UPDATE SET source_name=EXCLUDED.source_name 
                RETURNING source_id;
            '''.format(**{'source_table': 'lookup_' + source_suffix}))
    source_id=connection.execute(query, source_name=source_name).fetchone()[0]

    if load_patients:
        load_patient_data(connection, source_suffix)
    if load_events:
        load_facility_data(connection, source_suffix)
        load_event_data(connection, source_suffix)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError("Need argument for postgres_config.")

    to_load = sys.argv[2:]
    if len(to_load) == 0:
        load_patients = True
        load_events = True
    else:
        if "patients" in to_load:
            load_patients = True
        if "events" in to_load:
            load_events = True
        
    load_cdph_common_schema(sys.argv[1], load_patients, load_events)
