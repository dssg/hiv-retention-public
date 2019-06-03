-- Demographics
drop table if exists features_cs.demographics;
create table features_cs.demographics as
       (select distinct entity_id,
               d.update_date-1 as knowledge_date_column,
               date_of_birth, race_id, ethnicity_id, gender_id
        from patients_ucm.demographics d
        left join events_ucm.events using (entity_id)
        join events_ucm.gender using (event_id));
create index on features_cs.demographics(entity_id);
create index on features_cs.demographics(knowledge_date_column);

-- Location
drop table if exists features_cs.location;
create table features_cs.location as
       (select distinct entity_id,
               e.update_date-1 as knowledge_date_column,
               replace(replace(address_json, '''', E'\"'),'None','0')::json ->>'zipcode' as zip,
	       geocode as census_tract
        from events_ucm.address 
        left join events_ucm.events e using (event_id)
        where event_type = 'address');
create index on features_cs.location(entity_id);
create index on features_cs.location(knowledge_date_column);
