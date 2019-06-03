drop table features_cdph.trans_categ cascade;

create table features_cdph.trans_categ as
with trans_categ as (
select
"EHARSID",
transx_categ,
'1960-01-01'::date + "HIV_DX_DT"::int AS hiv_dx_dt,
'1960-01-01'::date + "AIDS_DX_DT"::int as aids_dx_dt,
'1960-01-01'::date + "DOB"::int dob_dt
from
raw_cdph.person
),
map as (
select
entity_id,
patient_id
from
patients_cdph.main)
select
a.entity_id,
b.transx_categ::int,
COALESCE(b.hiv_dx_dt,b.aids_dx_dt,b.dob_dt) as knowledge_date_col,
case when b.transx_categ::int = 1 then 'MSM'
     when b.transx_categ::int = 2 then 'IDU'
     when b.transx_categ::int = 3 then 'MSM_and_IDU'
     when b.transx_categ::int = 4 then 'clotting_factor'
     when b.transx_categ::int = 5 then 'Hetero_IDU'
     when b.transx_categ::int = 6 then 'Hetero_BIMale'
     when b.transx_categ::int = 7 then 'Hetero_Hemo'
     when b.transx_categ::int = 10 then 'Hetero_Transfer'
     when b.transx_categ::int = 11 then 'Hetero_PersonHIV'
     when b.transx_categ::int = 13 then 'Transfusion_Transplant'
     when b.transx_categ::int = 14 then 'Undetermined'
     when b.transx_categ::int = 15 then 'child_clotting_factor'
     when b.transx_categ::int = 16 then 'Mother_IDU'
     when b.transx_categ::int = 17 then 'Mother_Hetero_IDU'
     when b.transx_categ::int = 18 then 'Mother_Hetero_BIMale'
     when b.transx_categ::int = 19 then 'Mother_Hetro_Hemo'
     when b.transx_categ::int = 22 then 'Mother_Hetero_Contact_Transfusion'
     when b.transx_categ::int = 23 then 'Mother_Hetero_Contact_Male_HIV'
     when b.transx_categ::int = 24 then 'Mother_Rcvd_Transfusion_Transplant'
     when b.transx_categ::int = 25 then 'Mother_HIV'
     when b.transx_categ::int = 26 then 'Child_transfusion_transplant'
     when b.transx_categ::int = 27 then 'Child_undertermined'
     when b.transx_categ::int = 28 then 'Child_other_risk'
     when b.transx_categ::int = 88 then 'Adult_other_risk'
     when b.transx_categ::int = 99 then 'Risk_Factors_no_age'
     else 'no category' end as transmission_category
from
map as a
left join
trans_categ as b
on
a.patient_id = b."EHARSID";
