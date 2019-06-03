-- Study period is from 1/1/2008 to 8/30/2016

drop table if exists valid_visits;
create temp table valid_visits as
(
        select
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


-- self join of the visits to find the earliest and latest visit
-- in the succeeding year
drop table if exists events;
create table events as
(
        select
                x.entity_id, x.visit_date as outcome_date,
        case
                when max(y.visit_date) - min(y.visit_date) >= 90
                then false --is adherent; gets turned into 0
                else true -- not adherent; gets turned into 1
                end as outcome
        from valid_visits x
        join valid_visits y using (entity_id)
        where y.visit_date between x.visit_date and x.status_end_date
        group by x.entity_id, x.visit_date
);

drop table if exists states;
create table states as (
       with mrns as (
       select distinct entity_id, date_of_birth
       from patients_ucm.main
       join patients_ucm.demographics using (entity_id)
       )

       select distinct entity_id,
              visit_date as start_time,
              (visit_date + interval '1 day')::date as end_time,
              'active' as state
       from valid_visits
       join mrns using (entity_id)
       where visit_date > ((date_of_birth + interval '18 year')::date)
);
