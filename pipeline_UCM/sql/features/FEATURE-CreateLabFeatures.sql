-- Lab Features: Viral Load, CD4, CD8 and whether a patient has AIDS.
-- Note: We might want to do some of this in the data cleaning rather than here?

drop table if exists features_cs.viral_load;
create table features_cs.viral_load as
       (select
	distinct entity_id,
	lab_result_date as date_col,
        case
		when lab_test_value ilike '%credited%' or lab_test_value ilike '%NAT HIV REACTIV%' then NULL
                when lab_test_value ilike '%none detected%'  then 0
                when trim(lab_test_value) = 'No HIV-1 RNA detected' then 0
                when lab_test_value ilike '%less than 20%'  or lab_test_value ilike '%<20%'  then 0
                when replace(lab_test_value, ' ', '') like '%<50%'  then 50
                when replace(lab_test_value, ' ', '') like '%<5O%'  then 50 -- too account for a typo
                when replace(lab_test_value, ' ', '') like '%<75%'  then 75
                when replace(lab_test_value, ' ', '') = '<136'  then 136
                when replace(lab_test_value, ' ', '') like '%<250%'  then 250
  	        when replace(lab_test_value, ' ', '') like '%>10000%'  then 10000
		when replace(replace(lab_test_value, ' ', ''), ',', '') like '%>500000%'  then 500000
		when replace(lab_test_value, ',', '') ilike '%greater than 5000000%'  then 5000000
		when replace(lab_test_value, ',', '') like '%>10000000%'  then 10000000
                when (regexp_split_to_array(lab_test_value, 'Reference'))[1] ilike '%No HIV-1 RNA detected%'  then 0
		when replace(lab_test_value, ' ', '') = '' then 0
                else replace((regexp_split_to_array(replace(lower(lab_test_value), ' ', ''), 'copies'))[1], ',', '')::int
        end as lab_result
	from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'
              and trim(lower(test_type))='hiv-1 viral load hiv-rna level:'
	);
create index on features_cs.viral_load(entity_id);
create index on features_cs.viral_load(date_col);
create index on features_cs.viral_load(entity_id,date_col);


drop table if exists cd8;
create temp table cd8 as
       (select
               distinct entity_id,
       	       lab_result_date as date_col,
	       test_type,
	       case
                   when lab_test_value ilike '%credited%' then NULL
                   else lab_test_value::float
	       end as cd8
        from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'  and test_type like '%cd8%'
        );


drop table if exists cd4;
create temp table cd4 as
       (select
               distinct entity_id,
               lab_result_date as date_col,
               test_type,
               case
                   when lab_test_value ilike '%credited%' then NULL
                   else lab_test_value::float
               end as cd4
        from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'  and test_type like '%cd4%'
        );

drop table if exists features_cs.cd4cd8_ratio;
create table features_cs.cd4cd8_ratio as
       (select distinct cd4.entity_id,
       	       cd4.date_col,
	           cd4::float/cd8::float as cd4cd8_ratio,
	           cd8,
               cd4,
               case when cd4 < 200 then 1 else 0 end as aids
	     from cd8
	     join cd4
	          on cd4.entity_id = cd8.entity_id
	          and cd4.date_col = cd8.date_col
	          and replace(cd4.test_type, 'cd4', '') = replace(cd8.test_type, 'cd8', '')
       );

create index on features_cs.cd4cd8_ratio(entity_id);
create index on features_cs.cd4cd8_ratio(date_col);
create index on features_cs.cd4cd8_ratio(entity_id, date_col);
