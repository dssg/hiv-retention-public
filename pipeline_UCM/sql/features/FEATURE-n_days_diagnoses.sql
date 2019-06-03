drop table if exists features_cs.n_days_first_appt cascade;

create table features_cs.n_days_first_appt as (
with
first_appt as (
select
entity_id,
date_col + '1day'::interval as first_appt
from
features_cs.first_appt
where
first_appt_flag = 1),

appt_dates as (
select
entity_id,
start_time
from
states_for_predicting_appts_only)

select
entity_id,
start_time::date as date_col,
EXTRACT(epoch from (start_time-first_appt))/(3600*24)::int as n_days_first_appt
from
states_for_predicting_appts_only
left join
first_appt
using(entity_id)
order by
entity_id,
date_col
);

create index on features_cs.n_days_first_appt(entity_id);
create index on features_cs.n_days_first_appt(entity_id,date_col);
