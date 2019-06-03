drop table if exists staging.events_cdph_all_days_lab_vs cascade;
create table staging.events_cdph_all_days_lab_vs as
(
        select distinct on (x.entity_id,x.days,y.lab_date)
               x.entity_id,
               x.days as outcome_date,
               y.lab_date as lab_date,
               lab_result::float as viral_load,
               suppression
        from staging.all_days_labs x
        left join staging.cdph_cleaned_labs y on x.entity_id = y.entity_id
             and y.lab_date >  x.days
        where lab_type = 'EC-014' -- only viral loads
        order by x.entity_id, x.days, y.lab_date
);
\echo 'Created list of valid viral load tests that succeed each day';

drop table if exists staging.outcome_6mo;
create table staging.outcome_6mo as (
       with subset as (
            select distinct on (entity_id, outcome_date)
                   entity_id, outcome_date,
                   lab_date as lab_date_6mo,
                   suppression as vs_6mo,
                   viral_load as vl_6mo,
                   rank() over(partition by entity_id, outcome_date order by lab_date desc)
            from staging.events_cdph_all_days_lab_vs
            where lab_date <= outcome_date + '6months'::interval
            )
       select * from subset
       where rank = 1 
);
\echo 'Created outcomes for 6 months';

drop table if exists staging.outcome_12mo;
create table staging.outcome_12mo as (
       with subset as (
            select distinct on (entity_id, outcome_date)
                   entity_id, outcome_date,
                   lab_date as lab_date_12mo,
                   suppression as vs_12mo,
                   viral_load as vl_12mo,
                   rank() over(partition by entity_id, outcome_date order by lab_date desc)
            from staging.events_cdph_all_days_lab_vs
            where lab_date <= outcome_date + '12months'::interval
            )
       select * from subset
       where rank = 1 
);
\echo 'Created outcomes for 12 months';

drop table if exists staging.outcome_18mo;
create table staging.outcome_18mo as (
       with subset as (
            select distinct on (entity_id, outcome_date)
            entity_id, outcome_date,
            lab_date as lab_date_18mo,
            suppression as vs_18mo,
            viral_load as vl_18mo,
            rank() over(partition by entity_id, outcome_date order by lab_date desc)
            from staging.events_cdph_all_days_lab_vs
            where lab_date <= outcome_date + '18months'::interval
            )
       select * from subset
       where rank = 1 
);
\echo 'Created outcomes for 18 months';

drop table if exists events_cdph_suppression cascade;
create table events_cdph_suppression as
(
        select a.entity_id, a.days as outcome_date,
               case when vs_6mo then 0 else 1 end as outcome_vs_6mo,
               case when vs_12mo then 0 else 1 end as outcome_vs_12mo,
               case when vs_18mo then 0 else 1 end as outcome_vs_18mo
        from staging.all_days_labs a
        left join staging.outcome_6mo x6 on a.entity_id = x6.entity_id and a.days = x6.outcome_date
        left join staging.outcome_12mo x12 on a.entity_id = x12.entity_id and a.days = x12.outcome_date
        left join staging.outcome_18mo x18 on a.entity_id = x18.entity_id and a.days = x18.outcome_date
        order by 1,2
);
create index on events_cdph_suppression (entity_id,outcome_date);  
\echo 'Merged to create single viral suppression outcomes table';
