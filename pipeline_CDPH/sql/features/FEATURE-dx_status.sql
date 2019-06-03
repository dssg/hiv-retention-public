drop table if exists features_cdph.hiv_dx cascade;
create table features_cdph.hiv_dx as
with dx_status as (
select
    "EHARSID",
    dx_status,
    '1960-01-01'::date + "HIV_DX_DT"::int AS hiv_dx_dt,
    '1960-01-01'::date + "AIDS_DX_DT"::int as aids_dx_dt,
    '1960-01-01'::date + "DOB"::int dob_dt,
    case when dx_status  = '1'  then 1 else 0 end as hiv_dx,
    case when dx_status  = '2' then 1 else 0 end as aids_dx,
    case when dx_status = '3' then 1 else 0 end as perinatal_dx,
    case when dx_status = '4' then 1 else 0 end as pediatric_hiv_dx,
    case when dx_status = '5' then 1 else 0 end as pediatric_aids_dx
    from raw_cdph.person),
map as ( select entity_id, patient_id from patients_cdph.main )
select
    a.entity_id,
    COALESCE(hiv_dx_dt,aids_dx_dt,dob_dt) as knowledge_date_col,
    b.hiv_dx,
    b.aids_dx,
    b.perinatal_dx,
    b.pediatric_hiv_dx,
    b.pediatric_aids_dx
from
    map as a
left join dx_status as b on a.patient_id = b."EHARSID";

create index on features_cdph.hiv_dx(entity_id);
create index on features_cdph.hiv_dx(knowledge_date_col);
