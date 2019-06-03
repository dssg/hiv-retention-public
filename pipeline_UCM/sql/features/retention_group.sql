drop table if exists features_cs.retention cascade;

create table features_cs.retention as
with retention as
(
select
    entity_id,
    days,
    appt_date,
    retained,
    sum(retained) over (partition by entity_id order by days) as total_days_retained,
    sum(case when retained=0 then 1 else 0 end) over (partition by entity_id order by days) as total_days_unretained,
    (retained - lag(retained) over(partition by entity_id order by days)) as change_in_care,
    case when (retained - lag(retained) over(partition by entity_id order by days)) = -1 then 1 else 0 end as fell_out_of_retention,
    case when (retained - lag(retained) over(partition by entity_id order by days)) = 1 then 1 else 0 end as started_retention,
    case when (retained - lag(retained) over(partition by entity_id order by days)) <> 0 then 1 else 0 end as retention_change
from retained_by_day
),
retention_changes as (
select
    entity_id,
    days,
    appt_date,
    retained,
    total_days_retained,
    total_days_unretained,
    fell_out_of_retention,
    sum(fell_out_of_retention) over (partition by entity_id order by days) as n_drop_retention,
    sum(retention_change) over (partition by entity_id order by days) as n_changes_retention,
    started_retention
from
    retention),
retention_stats as (
select
    entity_id,
    days,
    appt_date,
    retained,
    total_days_retained,
    total_days_unretained,
    fell_out_of_retention,
    n_changes_retention,
    started_retention,
    n_drop_retention,
    sum(case when retained = 0 then 1 else 0 end) over (partition by entity_id,n_changes_retention order by days) as consecutive_noretention,
    sum(retained) over (partition by entity_id,n_changes_retention order by days) as consecutive_retention
from
    retention_changes)
select
    features_cs.id_appt.entity_id,
    features_cs.id_appt.date_col-'1day'::interval as date_col, --hack for triage
    days as feature_day,
    appt_date,
    retained,
    total_days_retained,
    total_days_unretained,
    fell_out_of_retention,
    n_changes_retention,
    started_retention,
    n_drop_retention,
    consecutive_noretention,
    consecutive_retention
from
    features_cs.id_appt

join
    retention_stats
on
    features_cs.id_appt.entity_id = retention_stats.entity_id
    and features_cs.id_appt.date_col = retention_stats.days
where
    completed = 1;

create index on features_cs.retention(entity_id);
create index on features_cs.retention(entity_id,date_col);

--We made the assumption the appt on that day is going to be completed.
