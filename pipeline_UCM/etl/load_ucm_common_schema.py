"""Load UCM common schema."""

import sys
import yaml
import itertools

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


def load_patients_info(connection, source_suffix):
    # Next stop: Main patients table.
    query = text('''
                with final_mrns as (
                        SELECT distinct
                            mrn::int,
                            to_date(enroll_date, 'MM/DD/YY') as enroll_date,
                            yearfirstvisit::int as yearfirstvisit,
                            to_date(firstvisitdt, 'MM/DD/YY') as firstvisitdt
                        FROM raw.final_mrns
                            where n_pop=1),

                cohort as (
                        SELECT DISTINCT
                            mrn,
                            to_date(dob, 'MM/DD/YYYY') as dob,
                            to_date(date_of_death, 'MM/DD/YYYY') as date_of_death,
                            sex,
                            race,
                            ethnicity,
                            address_line1,
                            address_line2,
                            city,
                            postal_code
                         FROM raw.cohort_diagnoses
                         JOIN final_mrns USING (mrn))

                INSERT INTO {target_table}.main (patient_id, source_id)
                        SELECT mrn, :source_id from cohort
                ON CONFLICT("patient_id","source_id") DO NOTHING;
                '''.format(**{'target_table': 'patients_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query, source_id=source_id)
    trans.commit()
    trans.close()

    # Next stop: Name table.
    query = text('''
                    with first_id_encounter_date as (
                        select mrn, min(start_date)::date as earliest_date
                            from raw.encounter_diagnoses
                            where enc_eio ilike '%o%'
                            group by 1)

                    insert into {target_table}.names (entity_id, update_date, full_name)
                    (select entity_id, earliest_date,
                            json_build_object('first_name', first_name,
                                                'last_name', last_name) AS full_name
                        from {target_table}.main as x
                        join first_id_encounter_date as y on x.patient_id::int=y.mrn
                        join raw.rw_mrns as z on x.patient_id::int=z.mrn)
                    ON CONFLICT DO NOTHING;
                '''.format(**{'target_table': 'patients_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # Next stop: Fill lookup_ucm.race.
    query = text('''
                    insert into {target_table}.race (race)
                    (select distinct race
                        from raw.cohort_diagnoses
                        where mrn in (
                                select patient_id::int
                                from patients_ucm.main));
                    '''.format(**{'target_table': 'lookup_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # Next stop: Fill lookup_ucm.ethnicity.
    query = text('''
                    insert into {target_table}.ethnicity (ethnicity)
                    (select distinct ethnicity
                        from raw.cohort_diagnoses
                        where mrn in (
                                select patient_id::int
                                from patients_ucm.main));
                    '''.format(**{'target_table': 'lookup_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # Next stop: Fill Demographics
    # This is a two-step process. Date of death can't be available before date of
    # death. Therefore, we have to add a second row for when date of death is
    # known. Update_date will be date of death for these rows.

    # first query with date of death set to 0
    query = text('''
                with first_id_encounter_date as (
                    select mrn, min(start_date)::date as earliest_date
                        from raw.encounter_diagnoses
                        where enc_eio ilike '%o%'
                group by 1)
                insert into {target_table}.demographics (entity_id, update_date,
                                                        date_of_birth, date_of_death,
                                                        race_id, ethnicity_id,
                                                        birth_address_id, death_address_id)
                select
                    entity_id,
                    earliest_date as update_date,
                    cohort.dob::date as date_of_birth,
                    null::date as date_of_death,
                    race_id,
                    ethnicity_id,
                    null as birth_address_id,
                    null as death_address_id
                from patients_ucm.main as x
                join first_id_encounter_date as y on x.patient_id::int=y.mrn
                join raw.rw_mrns as z on x.patient_id::int=z.mrn
                join (select distinct mrn, dob, date_of_death, race, ethnicity from raw.cohort_diagnoses) as cohort
                    on x.patient_id::int=cohort.mrn
                join lookup_ucm.race as race_table on cohort.race=race_table.race
                join lookup_ucm.ethnicity as ethnicity_table on cohort.ethnicity=ethnicity_table.ethnicity
                ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'patients_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # second query with date of death as update_date
    query = text('''
                with first_id_encounter_date as (
                    select mrn, min(start_date)::date as earliest_date
                        from raw.encounter_diagnoses
                        where enc_eio ilike '%o%'
                group by 1)
                insert into {target_table}.demographics (entity_id, update_date,
                                                        date_of_birth, date_of_death,
                                                        race_id, ethnicity_id,
                                                        birth_address_id, death_address_id)
                select entity_id,
                    date_of_death::date as update_date,
                    cohort.dob::date as date_of_birth,
                    date_of_death::date as date_of_death,
                    race_id,
                    ethnicity_id,
                    null as birth_address_id,
                    null as death_address_id
                from patients_ucm.main as x
                join first_id_encounter_date as y on x.patient_id::int=y.mrn
                join raw.rw_mrns as z on x.patient_id::int=z.mrn
                join (select distinct mrn, dob, date_of_death, race, ethnicity from raw.cohort_diagnoses) as cohort
                    on x.patient_id::int=cohort.mrn
                join lookup_ucm.race as race_table on cohort.race=race_table.race
                join lookup_ucm.ethnicity as ethnicity_table on cohort.ethnicity=ethnicity_table.ethnicity
                where date_of_death is not null
                ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'patients_' + source_suffix}))

    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

def load_gender(connection, source_suffix):
    # Gender (as an event)
    query = text('''
            with gender as (
                select entity_id, mrn,
                    case when sex='F' then 'female'
                         when sex='M' then 'male'
                    else 'other' 
                    end as sex,
                    dob
                from raw.cohort_diagnoses
                join patients_ucm.main 
                    ON mrn = patient_id::int
                ),
            first_id_encounter_date as (
                select mrn, min(start_date)::date as earliest_date
                    from raw.encounter_diagnoses_20170918
                    where enc_eio ilike '%o%'
            group by 1),
            events as (
                insert into {target_table}.events (event_type, entity_id, update_date)
                    select 'gender' as event_type, 
                        entity_id, 
                        earliest_date as update_date
                    from gender as x
                    join first_id_encounter_date as y 
                        on x.mrn::int=y.mrn
                returning event_id, entity_id, update_date
            )
            insert into {target_table}.gender ( event_id, gender_id )
            select  
                distinct event_id, sex::gender_enum 
            from events 
            join gender
                on events.entity_id = gender.entity_id 
                and update_date = dob::date
            ON CONFLICT DO NOTHING;
        '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

def add_ahrq_rollup_icd9(connection):
    # AHRQ rollups do not have consistent ICD9/10 codes so we try several versions 
    # to get a match
    # from:  https://www.hcup-us.ahrq.gov/toolssoftware/ccs/Single_Level_CCS_2015.zip
    df_data1 = pd.read_csv("/group/dsapp-lab/lookups/AHRQ_rollups/ICD9/Single_Level_CCS_2015/$dxref 2015.csv", \
                      na_values=["NULL"], header=1, quotechar='\'', quoting=1)
    # procedure codes shouldn't be used
    #df_data2 = pd.read_csv("/group/dsapp-lab/lookups/AHRQ_rollups/ICD9/Single_Level_CCS_2015/$prref 2015.csv", \
        #                      na_values=["NULL"], header=1, quotechar='\'', quoting=1)
    #df_data = pd.concat([df_data1, df_data2])
    df_data = df_data1
    df_data = df_data[['CCS CATEGORY', 'CCS CATEGORY DESCRIPTION', 'ICD-9-CM CODE']]
    query = text('''
                SELECT distinct icd9_dx, trim(lower(icd9_text)) as description
                FROM raw.diagnosis_diagnoses
                WHERE icd9_dx IS NOT NULL
            ''')
    icd9 = query_db(query, connection)
    # variations of the ICD9 codes in the roll-up
    icd9['rep1'] = icd9['icd9_dx'].str.replace(".", "").str.pad(5,side='right', fillchar=' ')
    icd9['rep2'] = icd9['icd9_dx'].str.replace(".", "").str.pad(5,side='left', fillchar='0')
    icd9['rep3'] = icd9['icd9_dx'].str.replace(".", "").str.pad(4,side='left', fillchar='0').str.pad(5,side='right', fillchar=' ')
    icd9['rep4'] = icd9['icd9_dx'].str.replace(".", "").str.pad(4,side='right', fillchar='0').str.pad(5,side='right', fillchar=' ')
    icd9['rep5'] = icd9['icd9_dx'].str.replace(".", "").str.pad(5,side='right', fillchar='0')

    to_keep = ['icd9_dx', 'description', 'rep1', 'rep2', 'rep3', 'rep4', 'rep5', 'ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']

    temp1 = pd.merge(icd9, df_data, how='left', left_on='rep1', right_on='ICD-9-CM CODE')
    temp2 = pd.merge(temp1, df_data, how='left', left_on='rep2', right_on='ICD-9-CM CODE')
    for col in ['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp2[col] = temp2[col+'_x'].fillna(temp2[col+'_y'])
    temp2 = temp2[to_keep]

    temp3 = pd.merge(temp2, df_data, how='left', left_on='rep3', right_on='ICD-9-CM CODE')
    for col in ['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp3[col] = temp3[col+'_x'].fillna(temp3[col+'_y'])
    temp3 = temp3[to_keep]

    temp4 = pd.merge(temp3, df_data, how='left', left_on='rep4', right_on='ICD-9-CM CODE')
    for col in ['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp4[col] = temp4[col+'_x'].fillna(temp4[col+'_y'])
    temp4 = temp4[to_keep]

    temp5 = pd.merge(temp4, df_data, how='left', left_on='rep5', right_on='ICD-9-CM CODE')
    for col in ['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp5[col] = temp5[col+'_x'].fillna(temp5[col+'_y'])
    temp5 = temp5[to_keep]

    temp5.to_sql("icd9_ahrq", engine, schema = 'lookup_ucm', if_exists='replace')

def add_ahrq_rollup_icd10(connection):
    # from:  https://www.hcup-us.ahrq.gov/toolssoftware/ccs10/ccs_dx_icd10cm_2018_1.zip
    # and : https://www.hcup-us.ahrq.gov/toolssoftware/ccs10/ccs_pr_icd10pcs_2018_1.zip
    df_data1 = pd.read_csv("/group/dsapp-lab/lookups/AHRQ_rollups/ICD10/ccs_pr_icd10pcs_2018_1.csv", \
                      na_values=["NULL"], quotechar='\'', quoting=1)
    df_data1 = df_data1.rename(index=str, columns={"ICD-10-PCS CODE": "ICD-10-CM CODE"})
    #df_data2 = pd.read_csv("/group/dsapp-lab/lookups/AHRQ_rollups/ICD10/ccs_dx_icd10cm_2018_1.csv", \
        #                      na_values=["NULL"], quotechar='\'', quoting=1)

    #df_data = pd.concat([df_data1[['ICD-10-CM CODE','CCS CATEGORY','CCS CATEGORY DESCRIPTION']], \
        #                     df_data2[['ICD-10-CM CODE','CCS CATEGORY','CCS CATEGORY DESCRIPTION']]])
    df_data = df_data1[['ICD-10-CM CODE','CCS CATEGORY','CCS CATEGORY DESCRIPTION']]
    query = text('''
                SELECT distinct icd10_dx, trim(lower(icd10_text)) as description
                FROM raw.diagnosis_diagnoses
                WHERE icd10_dx IS NOT NULL
            ''')
    icd10 = query_db(query, connection)
    icd10['rep1'] = icd10['icd10_dx']
    icd10['rep2'] = icd10['icd10_dx'].astype(str) + '0'
    icd10['rep3'] = icd10['icd10_dx'].astype(str) + 'A'

    to_keep = ['icd10_dx', 'description', 'rep1', 'rep2', 'rep3', 'ICD-10-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']

    temp1 = pd.merge(icd10, df_data, how='left', left_on='rep1', right_on='ICD-10-CM CODE')
    temp2 = pd.merge(temp1, df_data, how='left', left_on='rep2', right_on='ICD-10-CM CODE')
    for col in ['ICD-10-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp2[col] = temp2[col+'_x'].fillna(temp2[col+'_y'])
    temp2 = temp2[to_keep]

    temp3 = pd.merge(temp2, df_data, how='left', left_on='rep3', right_on='ICD-10-CM CODE')
    for col in ['ICD-10-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']:
        temp3[col] = temp3[col+'_x'].fillna(temp3[col+'_y'])
    temp3 = temp3[to_keep]

    temp3.to_sql("icd10_ahrq", engine, schema = 'lookup_ucm', if_exists='replace')

    
def load_addresses(connection, source_suffix):
    # add the different address types (more relevant for CDPH)
    query = text('''
            INSERT INTO {target_table}.address_type (address_type)
                VALUES('residential');
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)

    # Note: need to add addresses and geocodes
    # potential clean up geocode lookup before adding?

def load_diagnoses(connection, source_suffix):
    # Events: Visits
    # get icd 9/10 codes
    query = text('''
            INSERT INTO {target_table}.icd9_codes (icd9_code, icd9_text)
              SELECT distinct icd9_dx, icd9_text 
              FROM
                (SELECT distinct icd9_dx, trim(lower(icd9_text)) as icd9_text
                FROM raw.diagnosis_diagnoses_20170918
                WHERE icd9_dx IS NOT NULL
                UNION
                SELECT distinct icd9_dx, trim(lower(icd9_text)) as icd9_text
                FROM raw.diagnosis_diagnoses
                WHERE icd9_dx IS NOT NULL) as a
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)

    query = text('''
            INSERT INTO {target_table}.icd10_codes (icd10_code, icd10_text)
              SELECT distinct icd10_dx, icd10_text 
              FROM
                (SELECT distinct icd10_dx, trim(lower(icd10_text)) as icd10_text
                FROM raw.diagnosis_diagnoses_20170918
                WHERE icd10_dx IS NOT NULL
                UNION
                SELECT distinct icd10_dx, trim(lower(icd10_text)) as icd10_text
                FROM raw.diagnosis_diagnoses
                WHERE icd10_dx IS NOT NULL) as a
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)


    # include additional info about icd 9/10 codes
    query = text('''
            INSERT INTO {target_table}.icd9_info (icd9_code, sti, oi, 
                psychiatric_illness, pregnancy_related_dx, substance_use)
                SELECT distinct icd9_dx, 
                                sti, 
                                opportunistic_infection, 
                                psychiatric_illness, 
                                pregnancy_related_dx, substance_use
                FROM raw.diagnosis_diagnoses
                WHERE icd9_dx IS NOT NULL
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)

    query = text('''
            INSERT INTO {target_table}.icd10_info (icd10_code, sti, oi, 
                psychiatric_illness, pregnancy_related_dx, substance_use)
                SELECT distinct icd10_dx, 
                                sti, 
                                opportunistic_infection, 
                                psychiatric_illness, 
                                pregnancy_related_dx, substance_use
                FROM raw.diagnosis_diagnoses
                WHERE icd10_dx IS NOT NULL
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)
    
    add_ahrq_rollup_icd9(connection)
    add_ahrq_rollup_icd10(connection)

    # get encounter types
    query = text('''
            INSERT INTO {target_table}.encounter_types (encounter_type)
                SELECT distinct trim(lower(encounter_type))
                FROM raw.appt_status_20170918
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)
    # get visit statuses
    query = text('''
            INSERT INTO {target_table}.status_codes (status)
                SELECT distinct trim(lower(appt_status))
                FROM raw.appt_status_20170918
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    connection.execute(query)

    # visits -- come from appts
    # join with encounters for the diagnoses
    query = text('''
            CREATE TEMP TABLE visits AS  
                SELECT 
                    entity_id, 
                    bill_num as batch_id, -- become batch num
                    icd9_dx, icd10_dx,
                    trim(lower(icd9_text)) as icd9_text,
                    trim(lower(icd10_text)) as icd10_text,
                    dx_rank, enc_eio, 
                    raw.encounter_diagnoses_20170918.start_date as visit_date,
                    status_id as visit_status_id,
                    coalesce(id_provider, 0) as id_provider_flag,
                    encounter_type_id,
                    provider_id, insurance_id
                FROM raw.appt_status_20170918
                LEFT JOIN raw.encounter_diagnoses_20170918 
                    USING (bill_num)
                LEFT JOIN raw.diagnosis_diagnoses_20170918 
                    USING (bill_num)
                LEFT JOIN lookup_ucm.status_codes
                    ON status = lower(raw.appt_status_20170918.appt_status)
                LEFT JOIN lookup_ucm.encounter_types
                    ON lookup_ucm.encounter_types.encounter_type 
                        = lower(raw.appt_status_20170918.encounter_type)
                LEFT JOIN provider_info
                    ON trim(lower(raw.appt_status_20170918.attending_name)) = name_from_lookup
                LEFT JOIN lookup_ucm.insurance
                    ON lower(raw.encounter_diagnoses_20170918.fin_class) = insurance
                JOIN patients_ucm.main
                    ON patient_id = raw.appt_status_20170918.mrn::varchar;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # add an event for each visit
    query = text('''
            INSERT INTO {target_table}.events (entity_id, batch_id, icd9_code, icd10_code, 
                icd9_text, icd10_text, dx_rank, enc_eio, visit_date, visit_status_id, 
                id_provider_flag, encounter_type_id, provider_id, insurance_id, 
                source_id, event_type)
                SELECT 
                    entity_id, batch_id, icd9_dx, icd10_dx, 
                    icd9_text, icd10_text, dx_rank, enc_eio, visit_date::date, visit_status_id, 
                    id_provider_flag, encounter_type_id, provider_id, insurance_id,
                    {source_id} as source_id, 'visit'
                FROM visits
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix, 'source_id':source_id}))
    # this takes a while..
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()


def load_labs(connection, source_suffix):
    # get lab test types
    query = text('''
            INSERT INTO {target_table}.test_types (test_type, procedure_type, component_type)
                SELECT distinct trim(lower(proc_name)) || ' ' || trim(lower(component_name)), 
                    trim(lower(proc_name)), trim(lower(component_name))
                FROM raw.lab_diagnoses
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()
    
    # add u chicago as a lab
    query = text('''
            INSERT INTO {target_table}.labs (lab_name)
                VALUES('university of chicago')
                RETURNING lab_id;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    lab_id=connection.execute(query)    


    query = text('''
            CREATE TEMP TABLE labs AS  
                SELECT 
                    entity_id, 
                    bill_num as batch_id, -- become batch num
                    provider_id, 
                    coalesce(id_provider, 0) as id_provider_flag,
                    insurance_id,
                    start_date::DATE as lab_order_date,
                    result_time::DATE as lab_result_date,
                    test_type_id,
                    ord_value as lab_test_value,
                    {lab_id} as lab_id
                FROM raw.lab_diagnoses l 
                LEFT JOIN raw.encounter_diagnoses_20170918 USING (bill_num)
                LEFT JOIN lookup_ucm.provider_name p
                    ON p.provider_name = lower(raw.encounter_diagnoses_20170918.attending_name)
                LEFT JOIN lookup_ucm.id_providers id
                    ON id.provider_name = lower(raw.encounter_diagnoses_20170918.attending_name)
                LEFT JOIN lookup_ucm.test_types
                    ON trim(lower(proc_name)) || ' ' || trim(lower(component_name)) = test_type
                LEFT JOIN lookup_ucm.insurance
                    ON lower(fin_class) = insurance
                JOIN patients_ucm.main
                    ON patient_id = raw.encounter_diagnoses_20170918.mrn::varchar;
            '''.format(**{'target_table': 'events_' + source_suffix, 'lab_id':lab_id}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    # add an event for each lab
    query = text('''
            INSERT INTO {target_table}.events ( entity_id, batch_id,
                provider_id, id_provider_flag, insurance_id, lab_order_date, lab_result_date, 
                test_type_id, lab_test_value, lab_id, source_id, event_type )
                SELECT 
                    entity_id, batch_id,
                    provider_id, id_provider_flag, insurance_id, 
                    lab_order_date, lab_result_date, 
                    test_type_id, lab_test_value, lab_id,
                    {source_id} as source_id, 'lab'
                FROM labs
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix, 'source_id':source_id}))
    # this takes a while..
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()


def get_generic_name_helper(m):
    if m == 'glucagon':
        return(['glucagon recombinant'])
    if 'swab' in m:
        return(['swab'])
    if 'cholecalciferol' in m:
        return(['ergocalciferol'])
    if 'pyridoxine' in m:
        return(['ergocalciferol'])
    possible = recodes[recodes['name'] == m]
    if possible.shape[0] == 1:
        return possible['generic']
    possible = recodes[recodes['generic'] == m]
    if possible.shape[0] == 1:
        return possible['generic']
    possible = recodes[recodes['name'].str.contains(m)]
    if possible.shape[0] == 0:
        possible = recodes[recodes['generic'].str.contains(m)]
    return (possible['generic'].unique())

def get_generic_name(med_name):
    med_name = med_name.replace('(', '').replace(')', '')
    m1 = med_name.split(' ')[0]
    possible = get_generic_name_helper(m1)
    if len(possible) == 1:
        if not isinstance(possible, (list,)):
            possible = possible.tolist()
        return (possible[0])
    if m1 == med_name:
        return(med_name)
    m2 = med_name.split(' ')[0]+' '+med_name.split(' ')[1]
    possible = get_generic_name_helper(m2)
    if len(possible) == 1:
        if not isinstance(possible, (list,)):
            possible = possible.tolist()
        return (possible[0])
    m = "".join(itertools.takewhile(lambda x: 
                                    x.isalpha() or x.isspace() 
                                    or x=='-' or x==',', med_name))
    m = m.replace(' oral', '')
    return(m)

recodes = pd.read_csv("/group/dsapp-lab/lookups/brand_generics.csv")
recodes['name'] = recodes['name'].str.lower()
recodes['generic'] = recodes['generic'].str.lower()

def load_meds(connection, source_suffix):

    # get medications list
    query = text('''
                SELECT distinct trim(lower(med_name)) as ucm_med_name
                FROM raw.medication_diagnoses
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
    meds = query_db(query, connection)

    # converting brands to generics
    # from: https://www.fda.gov/Drugs/InformationOnDrugs/ucm129662.htm
    # https://www.fda.gov/downloads/Drugs/DevelopmentApprovalProcess/UCM071118.pdf

    meds['treatment_type'] = meds['ucm_med_name'].apply(get_generic_name)
    meds['ucm_med_name'] = meds['ucm_med_name'].astype('str') 
    meds['treatment_type'] = meds['treatment_type'].astype('str') 
    meds.to_sql("treatment_types", engine, schema = 'lookup_ucm', if_exists='replace')

    # medications
    query = text('''
            CREATE TEMP TABLE meds AS  
                SELECT 
                    entity_id, 
                    bill_num as batch_id, -- become batch num
                    m.start_date::DATE as treatment_start_date,
                    m.end_date::DATE as treatment_end_date,
                    provider_id, coalesce(id_provider, 0) as id_provider_flag,
                    lookup_ucm.treatment_types.index as treatment_type_id,
                    trim(lower(order_med_freq)) as treatment_frequency,
                    order_med_dose as treatment_dose,
                    art, psychiatric_medication, opioid, oi_prophylaxis
                FROM raw.medication_diagnoses m 
                LEFT JOIN raw.encounter_diagnoses_20170918 USING (bill_num) --need encounter to get to providers
                LEFT JOIN lookup_ucm.provider_name p
                    ON p.provider_name = lower(raw.encounter_diagnoses_20170918.attending_name)
                LEFT JOIN lookup_ucm.id_providers id
                    ON id.provider_name = lower(raw.encounter_diagnoses_20170918.attending_name)
                LEFT JOIN lookup_ucm.treatment_types
                    ON trim(lower(med_name)) = ucm_med_name
                JOIN patients_ucm.main
                    ON patient_id = raw.encounter_diagnoses_20170918.mrn::varchar;
            '''.format(**{'target_table': 'events_' + source_suffix}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()


    # add an event for each medicine
    query = text('''
            INSERT INTO {target_table}.events (entity_id, batch_id,
                    treatment_start_date, treatment_end_date,
                    provider_id, id_provider_flag, treatment_type_id,
                    treatment_frequency, treatment_dose,
                    art, psychiatric_medication, opioid, oi_prophylaxis, source_id, event_type)
                SELECT 
                    entity_id, batch_id,
                    treatment_start_date, treatment_end_date,
                    provider_id, id_provider_flag, treatment_type_id,
                    treatment_frequency, treatment_dose,
                    art, psychiatric_medication, opioid, oi_prophylaxis,
                    {source_id} as source_id, 'treatment'
                FROM meds
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'events_' + source_suffix, 'source_id':source_id}))
    trans = connection.begin()
    _ = connection.execute(query)
    trans.commit()
    trans.close()

    
def load_ucm_common_schema(postgres_config, load_patients, load_events):
    """Load UCM common schema.
    In:
        - postgres_config: path to postgres_config
    """
    engine = get_db_conn(postgres_config)
    connection = engine.connect()

    # We start with the source_id.
    source_suffix = 'ucm'
    source_name = 'uchicago_medicine'

    query = text('''
                 INSERT INTO {source_table}.data_source (source_name)
                    VALUES (:source_name)
                    ON CONFLICT("source_name") DO UPDATE SET source_name=EXCLUDED.source_name
                        RETURNING source_id;
                '''.format(**{'source_table': 'lookup_' + source_suffix}))
    source_id = connection.execute(query, source_name=source_name).fetchone()[0]


    if load_patients:
        load_patients_info(connection, source_suffix)

    if load_events:
        load_gender(connection, source_suffix)
        load_addresses(connection, source_suffix)


        # Always use the old appointments table to get the id_providers
        # id_provider status
        query = text('''
            CREATE TABLE {target_table}.id_providers AS  
                SELECT DISTINCT
                    trim(lower(attending_name)) as provider_name,
                    trim(lower(attending_service)) as provider_dept,
                    id_provider
                FROM raw.appt_status;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
        connection.execute(query)
    
        # add u chicago as a facility
        query = text('''
                INSERT INTO {target_table}.facilities (facility_name)
                    VALUES('university of chicago');
                '''.format(**{'target_table': 'lookup_' + source_suffix}))
        connection.execute(query)

        # add providers, departments and connections to facility
        query = text('''
                WITH providers as (
                    INSERT INTO {target_table}.provider_name (provider_name)
                        SELECT DISTINCT lower(attending_name)
                        FROM raw.appt_status
                        RETURNING provider_id, provider_name
                    )
                INSERT INTO {target_table}.provider_info(provider_id,
                                                            provider_dept,
                                                            source_id,
                                                            provider_facility_id)
                    SELECT provider_id, lower(attending_service),
                        1 as source_id, -- todo: fix this
                        1 as provider_facility_id
                    FROM providers
                    JOIN raw.encounter_diagnoses
                        ON provider_name = lower(attending_name)
                ON CONFLICT DO NOTHING;
                '''.format(**{'target_table': 'lookup_' + source_suffix}))
        connection.execute(query)

        query = text('''
            with name_mapping as (
                    select distinct trim(lower(x.attending_name)) as provider_name,
                                trim(lower(y.attending_name)) as matched_provider_name
                    from raw.appt_status_20170918 x
                    join raw.appt_status y
                            on (x.mrn=y.mrn and x.bill_num=y.bill_num 
                                and split_part(y.attending_name, ',', 1) = split_part(x.attending_name, ',', 1))),
            old_mrn_bill as (
                    select mrn, bill_num, start_date, 
                            attending_name, attending_service, 
                            matched_provider_name, provider_name
                    from staging.appt_status a
                    join name_mapping b 
                        on lower(trim(a.attending_name)) = b.matched_provider_name
                    where lower(attending_service) in ('infectious diseases', 'ped infectious diseases',
                                                        'internal medicine', 'hematology/oncology'))
    select distinct department_name, attending_service
    from raw.appt_status_20170918 x
    join old_mrn_bill y
    on x.mrn=y.mrn and x.bill_num=y.bill_num and 
    x.enc_date::date=y.start_date and lower(trim(x.attending_name))=y.provider_name;''')
        trans = connection.begin()
        _ = connection.execute(query)
        trans.commit()
        trans.close()

        # This is to account for certain tables using 
        # the provider name WITH middle name
        # and WITHOUT middle name
        query = text('''
            CREATE TEMP TABLE provider_info AS  
                SELECT 
                    distinct provider_id, x.provider_name as name_from_lookup, 
                    z.provider_name as name_from_idlist, 
                    matched_provider_name, max(z.id_provider) as id_provider
                FROM lookup_ucm.provider_name x 
                LEFT JOIN lookup_ucm.cleaned_id_provider_name y 
                    USING (provider_name)
                LEFT JOIN lookup_ucm.id_providers z
                    ON y.matched_provider_name = z.provider_name
                GROUP BY 1, 2, 3, 4
                    ;
            '''.format(**{'target_table': 'events_' + source_suffix}))
        trans = connection.begin()
        _ = connection.execute(query)
        trans.commit()
        trans.close()

        # get insurances
        query = text('''
            INSERT INTO {target_table}.insurance (insurance)
                SELECT distinct lower(fin_class)
                FROM raw.encounter_diagnoses_20170918
            ON CONFLICT DO NOTHING;
            '''.format(**{'target_table': 'lookup_' + source_suffix}))
        connection.execute(query)
    
        load_diagnoses(connection, source_suffix)
        load_labs(connection, source_suffix)
        load_meds(connection, source_suffix)

    print("Done.")
    return


if __name__ == '__main__':
    if len(sys.argv) != 2:
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
    
    load_ucm_common_schema(sys.argv[1], load_patients, load_events)
