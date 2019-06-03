-- Study period is from 1/1/2041 (??  check this)
-- currently using 3/3/2047 as last lab date

create schema staging;

-- TODO: needs to pull from common schema?
drop table if exists staging.cdph_cleaned_labs;
create table staging.cdph_cleaned_labs as
(
        select
                distinct
                entity_id,
                "RESULT_RPT_DT"::date as lab_date,
                lab_test_cd as lab_type,
                result as lab_result, result_units,
                case when (lab_test_cd='EC-014' and result::float <= 200) then TRUE else FALSE end as suppression 
        FROM raw_cdph.lab_with_dates
        JOIN raw_cdph.document using ("DOCUMENTID")
        JOIN patients_cdph.main
        ON "EHARSID" = patient_id
        WHERE lab_test_cd like 'EC-%'
              AND lab_test_cd is not NULL
              AND result is not NULL
              AND "RESULT_RPT_DT" is not NULL
              AND "RESULT_RPT_DT" > '01-01-2041'
);
\echo 'Created cleaned set of lab results';

drop table if exists staging.all_days_labs cascade;
create table staging.all_days_labs as
    select
        entity_id,
        generate_series(first_lab,last_lab,'1day') as days
    from (
         select entity_id,
                min(lab_date) as first_lab,
                '03-03-2047'::date as last_lab
         from staging.cdph_cleaned_labs
         group by entity_id) as a;
create index on staging.all_days_labs(entity_id);
create index on staging.all_days_labs(days);
\echo 'Created list of all relevant days';
