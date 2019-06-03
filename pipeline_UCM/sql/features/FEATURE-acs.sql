-- For each appointment date,
-- pull in relevant ACS feature
-- need a mapping of address to geo-id somewhere

drop table if exists features_cs.acs;
create table features_cs.acs as
    (select distinct entity_id,
            update_date-1 as date_col,
	    total_population, total_black,
	    total_black/(total_population+1) as frac_black,
	    native_born, native_born/(total_population+1) as frac_native_born,
	    foreign_born, foreign_born/(total_population+1) as frac_foreign_born,
	    mobility_same_house,mobility_same_house/(total_population+1) as frac_mobility_same_house,
	    mobility_moved_same_county,mobility_moved_same_county/(total_population+1) as frac_mobility_moved_same_county,
	    mobility_moved_diff_county_same_state,mobility_moved_diff_county_same_state/(total_population+1) as frac_mobility_moved_diff_county,
	    mobility_moved_diff_state,mobility_moved_diff_state/(total_population+1) as frac_mobility_moved_diff_state,
	    mobility_moved_abroad,mobility_moved_abroad/(total_population+1) as frac_mobility_moved_abroad,
	    with_ssisnap,with_ssisnap/(total_population+1) as frac_with_ssisnap,
	    ratio_lt_1_3,ratio_lt_1_3/(total_population+1) as frac_ratio_lt_1_3,
	    ratio_1_3_to_1_49,ratio_1_3_to_1_49/(total_population+1) as frac_ratio_1_3_to_1_49,
	    ratio_1_5_to_1_84,ratio_1_5_to_1_84/(total_population+1) as frac_ratio_1_5_to_1_84,
	    ratio_gte_1_85,ratio_gte_1_85/(total_population+1) as frac_ratio_gte_1_85,
	    income_lt_10k,income_lt_10k/(total_population+1) as frac_income_lt_10k,
	    income_10_to_15k,income_10_to_15k/(total_population+1) as frac_income_10_to_15k,
	    income_15_to_20k,income_15_to_20k/(total_population+1) as frac_income_15_to_20k,
	    income_20_to_25k,income_20_to_25k/(total_population+1) as frac_income_20_to_25k,
	    income_25_to_30k,income_25_to_30k/(total_population+1) as frac_income_25_to_30k,
	    income_30_to_35k,income_30_to_35k/(total_population+1) as frac_income_30_to_35k,
	    income_35_to_40k,income_35_to_40k/(total_population+1) as frac_income_35_to_40k,
	    income_40_to_45k,income_40_to_45k/(total_population+1) as frac_income_40_to_45k,
	    income_45_to_50k,income_45_to_50k/(total_population+1) as frac_income_45_to_50k,
	    income_50_to_60k,income_50_to_60k/(total_population+1) as frac_income_50_to_60k,
	    income_60_to_75k,income_60_to_75k/(total_population+1) as frac_income_60_to_75k,
	    income_75_to_100k,income_75_to_100k/(total_population+1) as frac_income_75_to_100k,
	    income_100_to_125k,income_100_to_125k/(total_population+1) as frac_income_100_to_125k,
	    income_125_to_150k,income_125_to_150k/(total_population+1) as frac_income_125_to_150k,
	    income_150_to_200k,income_150_to_200k/(total_population+1) as frac_income_150_to_200k,
	    income_gt_200k,income_gt_200k/(total_population+1) as frac_income_gt_200k,
	    travel_time_work,travel_time_work/(total_population+1) as frac_travel_time_work,
	    num_vehicles_to_work,num_vehicles_to_work/(total_population+1) as frac_num_vehicles_to_work,
	    travel_lt_5, travel_5_to_30, travel_30_to_60, travel_gt_60,
	    age_at_first_marriage_male, age_at_first_marriage_male/(total_population+1) as frac_age_at_first_marriage_male,
	    age_at_first_marriage_female, age_at_first_marriage_female/(total_population+1) as frac_age_at_first_marriage_female,
	    no_schooling, elementary_school, middle_school, high_school,
	    lt_1yr_college, some_college, associates, bachelors, masters, prof_degree, phd
     from events_ucm.events
     join
	(
	select entity_id, address_json, geocode
	from events_ucm.events 
	join events_ucm.address using (event_id)
	where event_type='address'
       	) as a
	using (entity_id)
     join lookup_acs.all_years
     	  on extract(year from update_date)-1 = extract(year from knowledge_date::date)
  	  and '14000US'||a.geocode = lookup_acs.all_years.geoid
     where event_type = 'address'
    );


-- don't use this table
drop table if exists features_cs.crimes;
create table features_cs.crimes as
(
	select distinct entity_id,
            update_date-1 as date_col,
	    geocode,
	    total_crimes as crimes,
	    total_arrests as arrests, total_non_arrests as non_arrests,
	    total_domestic as domestic, total_non_domestic as non_domestic,
	    -- drug
	    total_narcotics as narcotics,
	    total_criminal_trespass as criminal_trespass,
	    total_other_narcotic_violation as other_narcotic,
	    -- theft
	    total_theft as theft, total_motor_vehicle_theft as vehicle_theft,
	    total_burglary as burglary, total_robbery as robbery,
	    -- assault/damage
	    total_criminal_damage as criminal_damage, total_arson as arson,
	    total_homicide as homicide, total_kidnapping as kidnapping,
	    total_stalking as stalking, total_weapons_violation as weapons_violation,
	    total_battery as battery, total_assault as assault,
	    total_human_trafficking as human_trafficking,
	    total_concealed_carry_license_violation as concealed_carry,
	    total_domestic_violence as domestic_violence, total_interference_with_public_officer as interfere_officer,
	    -- sex crimes
	    total_prostitution as prostitution, total_crim_sexual_assault as crim_sexual_assault,
	    total_sex_offense as sex_offense, total_public_indecency as public_indecency,
	    total_obscenity as obscenity,
	    -- other
	    total_other_offense as other_offense,
	    total_deceptive_practice as deceptive_practice,
	    total_offense_involving_children as offense_with_children,
	    total_gambling as gambling,
	    total_public_peace_violation as public_peace,
	    total_liquor_law_violation as liquor_law,
	    total_intimidation as intimidation,
	    total_ritualism as ritualism,
	    "total_non-criminal" as non_criminal_1,
	    "total_non_-_criminal" as non_criminal_2,
	    "total_non-criminal_subject_specified" as noncriminal_subject_specified
	from events_ucm.events
	join
        (
        select entity_id, address_json, geocode
        from events_ucm.events
        join events_ucm.address using (event_id)
        where event_type='address'
        ) as a
        using (entity_id)
     join lookup_acs.crimes
          on update_date =  knowledge_date::date
          and a.geocode::text = census_tract::text
     where event_type = 'address'
);
