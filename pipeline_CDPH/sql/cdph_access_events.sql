-- join with all the possible prediction dates
-- and the valid labs to use for the outcomes

drop table if exists staging.events_cdph_labs_access cascade;
create table staging.events_cdph_labs_access as
(
        select distinct on (x.entity_id,x.days,y.lab_date)
                x.entity_id,
                x.days as outcome_date,
                y.lab_date as lab_date,
                case when (y.lab_date::date - x.days::date) <= 180  then false else true end as outcome_6months,
                case when (y.lab_date::date - x.days::date) <= 365  then false else true end as outcome_12months,
                case when (y.lab_date::date - x.days::date) <= 545  then false else true end as outcome_18months
        from staging.all_days_labs x
        left join staging.cdph_cleaned_labs y on x.entity_id = y.entity_id
        and y.lab_date >  x.days
        order by x.entity_id, x.days, y.lab_date
);
create index on staging.events_cdph_labs_access(entity_id);
create index on staging.events_cdph_labs_access(outcome_date);

drop table if exists events_cdph_access cascade;
create table events_cdph_access as
       with subset as (
            select
                entity_id,
                outcome_date,
                lab_date,
                outcome_6months,
                outcome_12months,
                outcome_18months,
                rank() over(partition by entity_id, outcome_date order by lab_date)
            from staging.events_cdph_labs_access
            )
       select entity_id, outcome_date, lab_date,
              outcome_6months, outcome_12months, outcome_18months
       from subset
       where rank = 1;
create index on events_cdph_access(entity_id,outcome_date);
