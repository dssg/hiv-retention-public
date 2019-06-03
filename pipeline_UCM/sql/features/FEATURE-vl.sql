-- Note: We might want to do some of this in the data cleaning rather than here?

drop table if exists vl cascade;
create temp table vl as
(select
distinct entity_id,
lab_result_date as date_col,
        case
		when lab_test_value ilike '%credited%' or lab_test_value ilike '%NAT HIV REACTIV%' then NULL
                when lab_test_value ilike '%none detected%'  then 0
                when trim(lab_test_value) = 'No HIV-1 RNA detected' then 0
                when lab_test_value ilike '%less than 20%'  or lab_test_value ilike '%<20%'  then 0
                when replace(lab_test_value, ' ', '') like '%<50%'  then 50
                when replace(lab_test_value, ' ', '') like '%<5O%'  then 50 -- to account for a typo
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

create temp table prev_lab as
(
        select entity_id, date_col, lab_result,
               lag(lab_result, 1) over (partition by entity_id order by date_col) as prev_lab_result
        from vl
);


drop table if exists features_cs.viral_load cascade;
create table features_cs.viral_load as
(select entity_id, date_col,
        lab_result,
        case when lab_result<=75 then 1 else 0 end as virally_supressed,
        case when lab_result>75  and lab_result<=200 then 1 else 0 end as vl_bn_75_200,
        case when lab_result>200 and lab_result<=10000 then 1 else 0 end as vl_bn_200_10k,
        case when lab_result>10000 and lab_result<=100000 then 1 else 0 end as vl_bn_10k_100k,
        case when lab_result>100000 then 1 else 0 end as vl_gt_100k,  
        round(log(lab_result+1)- log(prev_lab_result+1)) as magnitude_change
from prev_lab
);

create index on features_cs.viral_load(entity_id);
create index on features_cs.viral_load(date_col);
create index on features_cs.viral_load(entity_id, date_col);
