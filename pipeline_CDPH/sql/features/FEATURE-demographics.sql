--demographic features CDPH
drop table if exists features_cdph.demographics cascade;
create table features_cdph.demographics as
with first_labs as (
select distinct on (entity_id)
    entity_id,
    start_time::date as knowledge_date_col
from
    states_cdph_quarter
order by
      1,2
),
demographics as (
select
    id,
    entity_id,
    date_of_birth::date,
    race_id,
    ethnicity_id,
    gender_id
from
    patients_cdph.demographics
left join
     events_cdph.gender
on
    event_id = id
)
select
    first_labs.entity_id,
    first_labs.knowledge_date_col,
    demographics.date_of_birth,
    demographics.race_id,
    demographics.ethnicity_id,
    demographics.gender_id
from
    first_labs
left join
    demographics
using
    (entity_id);

create index on features_cdph.demographics(entity_id);
create index on features_cdph.demographics(knowledge_date_col);
