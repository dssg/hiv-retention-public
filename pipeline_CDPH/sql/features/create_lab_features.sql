-- What is the viral load?
-- Note: We might want to do some of this in the data cleaning rather than here?

drop table if exists features_cdph.viral_load;
create table features_cdph.viral_load as
       (select
	distinct entity_id,
	update_date as date_col,
	case when lab_test_value like '%7.500.000%' then '7500'::float
	     else replace(lab_test_value, '  Copies/mL', '')::float
	end  as vl
	from events_cdph.events
        left join raw_cdph.lab_test_code
             on lab_test_type_cd = code
        where event_type = 'lab'
              and trim(lower(description)) ilike '%viral load%'
	);
create index on features_cdph.viral_load(entity_id);
create index on features_cdph.viral_load(date_col);


drop table features_cdph.cd4;
create table features_cdph.cd4 as
       (select
               distinct entity_id,
               update_date as date_col,
	       replace(lab_test_value, '  Count', '')::float as cd4
               --case
               --    when lab_test_value ilike '%credited%' then NULL
               --    else lab_test_value::float
               --end as cd4
        from events_cdph.events
        left join raw_cdph.lab_test_code
             on lab_test_type_cd = code
        where event_type = 'lab'
              and trim(lower(description)) ilike '%CD4 T-lymphocytes%'
	      and update_date is not null
        );
create index on features_cdph.cd4(entity_id);
create index on features_cdph.cd4(date_col);

