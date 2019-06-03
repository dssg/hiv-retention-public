drop table if exists features_cdph.vl_appts cascade;
create table features_cdph.vl_appts as
select
    entity_id,
    date_col,
    1 as completed,
    date_col - LAG(date_col) over (partition by entity_id order by date_col) as days_bn_appts
from(
select
    entity_id,
    update_date as date_col
from
    events_cdph.events
where
    event_type = 'lab'
    and lab_test_type_cd like 'EC-014%'
    and update_date is not null
    group by 1,2) as a;

create index on features_cdph.vl_appts(entity_id);
create index on features_cdph.vl_appts(date_col);
