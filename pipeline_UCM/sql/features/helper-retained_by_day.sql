drop table if exists retention_dates cascade;

create temp table retention_dates as
select  a_entity_id,
        a_date_col,
        retention_appt,
        case
            when retention_appt is not null then
            (a_date_col + '365days'::interval)::date
            else null end as retention_end,
        (a_date_col + '365days'::interval)::date - retention_appt as days_between,
        case when retention_appt is null then 0 else 1 end as retained
from (

select
    a.entity_id as a_entity_id,
    a.date_col as a_date_col,
    b.date_col as retention_appt
from features_cs.id_appt as a
left join features_cs.id_appt as b
on a.date_col < b.date_col
and a.entity_id = b.entity_id
and b.date_col between a.date_col + '90days'::interval
and a.date_col + '365days'::interval
where a.completed = 1) as a
order by a_entity_id, a_date_col;

--

drop table if exists retained_days cascade;

create temp table retained_days as
select
    distinct
    entity_id,
    array_agg(distinct a_date_col) as appt_date,
    retained_day,
    retained
from (
select
    a_entity_id as entity_id,
    a_date_col,
    retention_appt,
    retention_end,
    generate_series(retention_appt,retention_end,'1day') as retained_day,
    days_between,
    retained
from
    retention_dates) as x
group by entity_id,retained_day, retained;

--

drop table if exists all_days_appts cascade;

create temp table all_days_appts as
select
entity_id,
generate_series(first_appt,last_appt,'1day') as days
from (
select
entity_id,
min(date_col) as first_appt,
max(date_col) as last_appt
from features_cs.id_appt
where completed = 1
group by entity_id) as a;


\echo 'creating big table: retained_by_day';

drop table if exists retained_by_day cascade;

create table retained_by_day as
select
    all_days_appts.entity_id,
    days,
    retained_days.appt_date,
    coalesce(retained,0) as retained
from
    all_days_appts
left join
     retained_days
on
    all_days_appts.entity_id = retained_days.entity_id
    and all_days_appts.days = retained_days.retained_day;

create index on retained_by_day(entity_id);
