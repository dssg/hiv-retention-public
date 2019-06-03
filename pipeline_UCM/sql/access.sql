-- Study period is from 1/1/2008 to 8/30/2016

drop table if exists valid_visits;
create temp table valid_visits as
(
        select distinct
                entity_id, visit_date
                --visit_date + '12 months'::interval - '1 day'::interval as status_end_date
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
drop table if exists access_ucm;
create table access_ucm as
(
        with appts_joined as
        (
        select distinct
                x.entity_id, x.visit_date as outcome_date,
                y.visit_date as visit_date
        from valid_visits x
        left join valid_visits y using (entity_id)
        where y.visit_date between
              x.visit_date
              and x.visit_date+'12months'::interval-'1day'::interval
        )
        select entity_id,
               outcome_date,
               case when count(*) > 1 then 0
               else 1 end as no_access_12mo
        from appts_joined
        group by 1,2
        order by 1,2
);

drop table if exists access_ucm_6months;
create table access_ucm_6months as
(
with appts_joined as
(
select distinct
x.entity_id, x.visit_date as outcome_date,
y.visit_date as visit_date
from valid_visits x
left join valid_visits y using (entity_id)
where y.visit_date between
x.visit_date
and x.visit_date+'6months'::interval-'1day'::interval
)
select entity_id,
outcome_date,
case when count(*) > 1 then 0
else 1 end as no_access_6mo
from appts_joined
group by 1,2
order by 1,2
);
