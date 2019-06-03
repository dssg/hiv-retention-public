/* First Appt */
drop table if exists features_cs.first_appt;
create table features_cs.first_appt as
(
        with first_appt as (
        select entity_id,
               (min(visit_date)-'1day'::interval)::date as date_col,
               1 as first_appt_flag
        from events_ucm.events
        where event_type = 'visit'
              and visit_date is not null
              and id_provider_flag = 1
              and visit_date > '1990-01-01'
        group by 1
        ),
        all_appt as (
        select distinct entity_id,
               (visit_date-'1day'::interval)::date as date_col
        from events_ucm.events
        where event_type = 'visit'
              and visit_date is not null
              and id_provider_flag = 1
              and visit_date > '1990-01-01'
        )

        select distinct entity_id,
               date_col,
               case when first_appt_flag is not null then 1
               else 0 end as first_appt_flag
        from all_appt
        left join first_appt using(entity_id, date_col)
);


create index on features_cs.first_appt(entity_id);
create index on features_cs.first_appt(date_col);
create index on features_cs.first_appt(entity_id, date_col); 
