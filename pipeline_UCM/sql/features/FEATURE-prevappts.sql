create temp table valid_appts as
(
        select distinct entity_id, visit_date as date_col, 
                case when status in ('completed', 'arrived', 'y') then 1 else 0 end as completed,
                --1 as scheduled,
                case when status = 'canceled' then 1 else 0 end as cancelled,
                case when status = 'no show' then 1 else 0 end as noshow,
                id_provider_flag
        from events_ucm.events
        join lookup_ucm.status_codes
             on status_id = visit_status_id
        where event_type = 'visit'
              and visit_date is not null
              and visit_date > '1990-01-01'
);

drop table if exists features_cs.appt;
create table features_cs.appt as
(
        select * ,
               date_col - LAG(date_col) over (partition by entity_id order by date_col) as days_bn_appts
        from valid_appts 
        where id_provider_flag = 0
);
create index on features_cs.appt(entity_id);
create index on features_cs.appt(date_col);
create index on features_cs.appt(entity_id,date_col);

drop table if exists features_cs.id_appt;
create table features_cs.id_appt as
(
        select * ,
               date_col - LAG(date_col) over (partition by entity_id order by date_col) as days_bn_appts
        from valid_appts 
        where id_provider_flag = 1
);
create index on features_cs.id_appt(entity_id);
create index on features_cs.id_appt(date_col);
create index on features_cs.id_appt(entity_id,date_col);
