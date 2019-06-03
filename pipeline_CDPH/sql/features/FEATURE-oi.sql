drop table if exists features_cdph.oi cascade;

create table if not exists features_cdph.oi as
select
    b.entity_id,
    a.oi_seq,
    a.oi_cd,
    a.dx,
    1 as oi,
    '1960-01-01'::date + a."DX_DT"::int as knowledge_date,
    case when oi_cd in ('AD15','AD16','AD17','AD18') then 'lymphoma'
         when oi_cd in ('AD20','AD21') then 'tuberculosis'
         when oi_cd in ('AD23','AD24') then 'pneumonia'
         when oi_cd in ('AD27') then 'toxoplasmosis'
         else 'other_oi'end as oi_type
from
    raw_cdph.oi a
left join staging.eharsid_documentid_entityid b using("DOCUMENTID")
where
    dx = 'D' and
    "DX_DT" is not null;

create index on features_cdph.oi(entity_id);
