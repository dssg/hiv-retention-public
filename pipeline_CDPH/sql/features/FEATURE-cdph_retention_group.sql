drop table if exists features_cdph.dayretention cascade;

create table features_cdph.dayretention as
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
from cdph_retained_by_day
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
time_for_retention_unnest as (
select
    entity_id, days, unnest(appt_date) as retention_day
from retention_changes
),
time_for_retention as (
select
    entity_id, days,
    max(retention_day) + '365days'::interval - days::date as n_days_left_retention
from time_for_retention_unnest
group by entity_id, days
),
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
    sum(retained) over (partition by entity_id,n_changes_retention order by days) as consecutive_retention,
    total_days_retained/(total_days_retained+total_days_unretained) as frac_retained,
    total_days_unretained/(total_days_retained+total_days_unretained) as frac_unretained
from
    retention_changes)
select
    retention_stats.entity_id,
    retention_stats.days-'1day'::interval as date_col,
    appt_date,
    retained,
    total_days_retained,
    total_days_unretained,
    fell_out_of_retention,
    n_changes_retention,
    started_retention,
    n_drop_retention,
    consecutive_noretention,
    consecutive_retention,
    n_days_left_retention,
    frac_retained,
    frac_unretained
from
    retention_stats
left join time_for_retention
on
    retention_stats.entity_id = time_for_retention.entity_id
    and retention_stats.days =  time_for_retention.days;

create index on features_cdph.dayretention(entity_id);
create index on features_cdph.dayretention(entity_id,date_col);

--We made the assumption the appt on that day is going to be completed.
