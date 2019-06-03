-- Demographics
drop table if exists features_cdph.demographics;
create table features_cdph.demographics as
       (select distinct entity_id,
               e.update_date-1 as knowledge_date_column,
               date_of_birth, race_id, ethnicity_id, gender_id
        from patients_cdph.demographics d
        left join events_cdph.events e using (entity_id)
        join events_cdph.gender using (event_id)
        where event_type='gender'
        );
create index on features_cdph.demographics(entity_id);
create index on features_cdph.demographics(knowledge_date_column);

-- Location
drop table if exists temp_loc;
create table temp_loc as
       (select distinct entity_id,
               e.update_date-1 as knowledge_date_column,
               substring(address_json::json->>'zipcode' from 1 for 5) as zip
        from events_cdph.address
        left join events_cdph.events e using (event_id)
        where event_type = 'address'
              and address_seq = 1);
drop table temp_zip;
create temp table temp_zip as
       (select zip
        from temp_loc
        group by zip
        order by count(zip) desc limit 20
       );

drop table if exists features_cdph.location;
create table features_cdph.location as
       (select distinct entity_id,knowledge_date_column,
	       case when zip in (select zip from temp_zip) then zip
	       	    when zip is null then 'unknown'
	       	    else 'other'
		    end as zip
        from temp_loc);
create index on features_cdph.location(entity_id);
create index on features_cdph.location(knowledge_date_column);
