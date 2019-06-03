-- Attendings
drop table if exists features_cdph.providers;
create table features_cdph.providers as
       (select distinct entity_id,
               update_date as date_col,
               provider_id
        from events_cdph.events
        where event_type='visit' or event_type='lab'
              and visit_date IS NOT NULL);
create index on features_cdph.providers(entity_id);
create index on features_cdph.providers(date_col);


-- Facilities
drop table if exists features_cdph.facilities;
create table features_cdph.facilities as
       (select distinct entity_id,
               update_date as date_col,
               facility_id
        from events_cdph.events
        where event_type='visit' or event_type='lab'
              and visit_date IS NOT NULL);
create index on features_cdph.facilities(entity_id);
create index on features_cdph.facilities(date_col);

-- need to add in insurance
-- Insurance
--drop table if exists features_cdph.insurance;
--create table features_cdph.insurance as
--       (select distinct entity_id, visit_date as date_col,
--               insurance_id
--        from events_cdph.events
--	where event_type='visit'
--       	     and visit_date IS NOT NULL);
--create index on features_cdph.insurance(entity_id);
--create index on features_cdph.insurance(date_col);
