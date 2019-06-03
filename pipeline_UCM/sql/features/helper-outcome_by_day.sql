drop table if exists valid_visits;
create temp table valid_visits as
(
        select distinct
                entity_id, visit_date,
                visit_date + '12 months'::interval - '1 day'::interval as status_end_date
        from events_ucm.events
        join lookup_ucm.status_codes
             on visit_status_id = status_id
        where id_provider_flag = 1
              and event_type='visit'
              and status = 'completed'
              and enc_eio ilike '%o%'
              and visit_date between '1/1/2008' and '8/30/2016'
);      

drop table if exists all_days_appts cascade;

create temp table all_days_appts as
select
        entity_id,
        generate_series(first_appt,last_appt,'1day') as days
from
( select
        entity_id,
        min(visit_date) as first_appt,
        max(visit_date) as last_appt
  from valid_visits
  group by entity_id) as a;   


drop table if exists outcome_by_day;
create table outcome_by_day as
(
        select
                y.entity_id, y.days as outcome_date,
                min(x.visit_date) as min_d,
                max(x.visit_date) as max_d,
                case
                        when max(x.visit_date) - min(x.visit_date) >= 90
                        then 0 --is adherent; gets turned into 0
                        else 1 -- not adherent; gets turned into 1
                end as outcome
        from all_days_appts y
        left join valid_visits x using (entity_id)
        where x.visit_date between y.days and (y.days+'12 months'::interval - '1 day'::interval)
        group by y.entity_id, y.days
 );                         

