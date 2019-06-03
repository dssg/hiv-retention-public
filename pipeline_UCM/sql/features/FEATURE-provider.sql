-- Hospital stays
drop table if exists features_cs.hospital_stays;
create table features_cs.hospital_stays as
       (select distinct entity_id,
               visit_date as date_col,
               1 as hospital
        from events_ucm.events
        where event_type='visit'
              and encounter_type_id=13
	      and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create index on features_cs.hospital_stays(entity_id);
create index on features_cs.hospital_stays(date_col);

-- Attendings
drop table if exists features_cs.providers;
create table features_cs.providers as
       (select distinct entity_id,
               visit_date as date_col,
               provider_id
        from events_ucm.events
        where event_type='visit'
              and id_provider_flag=1
              and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create index on features_cs.providers(entity_id);
create index on features_cs.providers(date_col);


-- Facilities
drop table if exists features_cs.facilities;
create table features_cs.facilities as
       (select distinct entity_id,
               visit_date as date_col,
               facility_id
        from events_ucm.events
        where event_type='visit'
              and visit_date IS NOT NULL
              and visit_date > '1990-01-01');
create index on features_cs.facilities(entity_id);
create index on features_cs.facilities(date_col);

-- Insurance
drop table if exists features_cs.insurance;
create table features_cs.insurance as
       (select distinct entity_id, visit_date as date_col,
               insurance_id
        from events_ucm.events
	where event_type='visit'
       	     and visit_date IS NOT NULL
             and visit_date > '1990-01-01');
create index on features_cs.insurance(entity_id);
create index on features_cs.insurance(date_col);
