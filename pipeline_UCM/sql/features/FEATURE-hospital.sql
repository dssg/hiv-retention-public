-- Hospital stays
drop table if exists features_cs.hospital_stays;
create table features_cs.hospital_stays as
       (select distinct entity_id,
               visit_date as date_col,
               1 as hospital,
               case when id_provider_flag = 1
                    then 1
                    else 0 end as id_provider_seen
        from events_ucm.events
	join lookup_ucm.encounter_types using (encounter_type_id)
        where event_type='visit'
              and encounter_type = 'hospital encounter'
	      and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create index on features_cs.hospital_stays(entity_id);
create index on features_cs.hospital_stays(date_col);
create index on features_cs.hospital_stays(entity_id, date_col);
