-- Medications
drop table if exists features_cs.meds cascade;
create table features_cs.meds as
       (select distinct entity_id,
               treatment_start_date as date_col,
	           --treatment_type_id as medication,
               case
               when (art::bool=False)
                    and (psychiatric_medication::bool=False)
                    and (opioid::bool=False)
                    and (oi_prophylaxis::bool=False)
                    then treatment_type_id
                    else NULL
               end as med_other,
               case when art::bool then treatment_type_id else NULL end as med_art,
               case when art::bool then treatment_type else NULL end as med_generic_art,
               case when psychiatric_medication::bool then treatment_type_id else NULL end as med_psych,
               case when opioid::bool then treatment_type_id else NULL end as med_opioid,
               case when oi_prophylaxis::bool then treatment_type_id else NULL end as med_oi
        from events_ucm.events
        join lookup_ucm.treatment_types
             on treatment_type_id = index
        where event_type='treatment'
	     and treatment_start_date is not null
	);
create index on features_cs.meds(entity_id);
create index on features_cs.meds(date_col);
create index on features_cs.meds(entity_id,date_col);
