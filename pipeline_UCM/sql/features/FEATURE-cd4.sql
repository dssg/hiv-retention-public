drop table cd8;
create temp table cd8 as
       (select
               distinct entity_id,
       	       lab_result_date as date_col,
	       test_type,
	       case
                   when lab_test_value ilike '%credited%' then NULL
                   else lab_test_value::float
	       end as cd8
        from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'  and test_type like '%cd8%'
        );
drop table cd4;
create temp table cd4 as
       (select
               distinct entity_id,
               lab_result_date as date_col,
               test_type,
               case
                   when lab_test_value ilike '%credited%' then NULL
                   else lab_test_value::float
               end as cd4
        from events_ucm.events
        left join lookup_ucm.test_types
             on lab_test_type_cd = test_type_id
        where event_type = 'lab'  and test_type like '%cd4%'
        );

drop table if exists features_cs.cd4cd8_ratio cascade;
create table features_cs.cd4cd8_ratio as
       (select distinct cd4.entity_id,
       	       cd4.date_col,
	       cd4::float/cd8::float as cd4cd8_ratio,
	       cd8,
           cd4,
               case when cd4<=200 then 1 else 0 end as aids,
               case when cd4>200 and cd4<=500 then 1 else 0 end as cd4_bn_200_500,
               case when cd4>500 then 1 else 0 end as cd4_gt_500,
               case when cd4::float/cd8::float <= 1 then 1 else 0 end as cd4cd8_ratio_lt_1,
               case when cd4::float/cd8::float > 1 and cd4::float/cd8::float <= 2 then 1 else 0 end as cd4cd8_ratio_bn_1_2,
               case when cd4::float/cd8::float > 2 then 1 else 0 end as cd4cd8_ratio_gt_2
	from cd8
	join cd4
	     on cd4.entity_id = cd8.entity_id
	     and cd4.date_col = cd8.date_col
	     and replace(cd4.test_type, 'cd4', '') = replace(cd8.test_type, 'cd8', '')
       );
create index on features_cs.cd4cd8_ratio(entity_id);
create index on features_cs.cd4cd8_ratio(date_col);
create index on features_cs.cd4cd8_ratio(entity_id, date_col);
