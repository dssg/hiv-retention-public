-- Study period is from 1/1/2041 (??  check this)

drop table if exists states_cdph cascade;
create table states_cdph as (
select
    entity_id,
    min(lab_date) as start_time,
    max(lab_date) +'1year'::interval as end_time,
    'active' as state
from
    staging.cdph_cleaned_labs
where
    lab_type in ('EC-016','EC-014')
group by
    entity_id
);
create index on states_cdph(entity_id);
