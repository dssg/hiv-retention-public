
-- What are diagnosis features?
drop table if exists features_cs.diag_info;
create table features_cs.diag_info as
       (select distinct entity_id,
               visit_date as date_col,
               coalesce(info9.psychiatric_illness, info10.psychiatric_illness) as psychiatric_illness,
	       coalesce(info9.oi, info10.oi) as oi,
	       coalesce(info9.sti, info10.sti) as sti,
	       coalesce(info9.substance, info10.substance_use) as substance
        from events_ucm.events
	left join lookup_ucm.icd9_info info9 USING (icd9_code)
	left join lookup_ucm.icd10_info info10 USING (icd10_code)
        where event_type = 'visit' and visit_date IS NOT NULL and visit_date > '1990-01-01');
create index on features_cs.diag_info(entity_id);
create index on features_cs.diag_info(date_col);

-- Diagnoses list
create temp table temp_diag as
       (select distinct entity_id, visit_date as date_col,
               coalesce(icd9_code, icd10_code) as diagnosis,
               coalesce(a."CCS CATEGORY", b."CCS CATEGORY") as ccs_cat
        from events_ucm.events
        left join lookup_ucm.icd9_ahrq a
             on icd9_code = icd9_dx
        left join lookup_ucm.icd10_ahrq b
             on icd10_code = icd10_dx
        where event_type='visit'
              and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create temp table temp_diag2 as
       (select diagnosis
        from temp_diag
        group by diagnosis
        order by count(diagnosis) desc limit 25
       );
create temp table temp_diag3 as
       (select ccs_cat
        from temp_diag
        group by ccs_cat
        order by count(ccs_cat) desc limit 25
       );
drop table if exists features_cs.diagnosis_code;
create table features_cs.diagnosis_code as
       (select distinct entity_id, date_col,
               case
	       	    when diagnosis in (select diagnosis
		                 from temp_diag2)
			then diagnosis
	       	    else 'other'
	       end as diagnosis,
               case
	       	    when ccs_cat in (select ccs_cat
		                 from temp_diag3)
			then ccs_cat
	       	    else 0
	       end as ccs_cat
        from temp_diag);
create index on features_cs.diagnosis_code(entity_id);
create index on features_cs.diagnosis_code(date_col);

-- Hospital stays
drop table if exists features_cs.hospital_stays;
create table features_cs.hospital_stays as
       (select distinct entity_id,
               visit_date as date_col,
               1 as hospital
        from events_ucm.events
	join lookup_ucm.encounter_types using (encounter_type_id)
        where event_type='visit'
              and encounter_type = 'hospital encounter'
	      and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create index on features_cs.hospital_stays(entity_id);
create index on features_cs.hospital_stays(date_col);

drop table positive_substance;                                                                                                             
create temp table positive_substance as                                                                                                    
       (                                                                                                                                   
        select distinct entity_id,                                                                                                         
               lab_result_date as date_col,                                                                                                
               case when lab_test_value ilike '%positive%' then 1                                                                          
               else 0 end as tox_pos                                                                                                       
        from events_ucm.events                                                                                                             
        left join lookup_ucm.test_types                                                                                                    
             on lab_test_type_cd = test_type_id                                                                                            
        where event_type = 'lab'                                                                                                           
              and test_type ilike '%toxicology%'                                                                                           
       );                                                                                                                                  


drop table if exists features_cs.diagnoses;
create table features_cs.diagnoses as
       (
       select *
       from features_cs.diag_info
       full outer join features_cs.diagnosis_code using (entity_id, date_col)
       full outer join positive_substance using (entity_id, date_col)
       );
create index on features_cs.diagnoses(entity_id);
create index on features_cs.diagnoses(date_col);
