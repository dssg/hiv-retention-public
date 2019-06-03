-- For each appointment date,
-- pull in relevant ACS feature
-- need a mapping of address to geo-id somewhere

drop table if exists features_cs.crimes;
create table features_cs.crimes as
(
	select entity_id, "Date"::date as date_col,
	       1 as crime,
	       case when "Primary Type"='THEFT' then 1 else 0 end as theft,
	       case when "Primary Type"='INTERFERENCE WITH PUBLIC OFFICER' then 1 else 0 end as interfere,
	       case when "Primary Type"='NARCOTICS' then 1 else 0 end as narcotics,
	       case when "Primary Type"='NON-CRIMINAL' then 1 else 0 end as non_crim,
	       case when "Arrest"='t' then 1 else 0 end as arrest,
   	       case when "Domestic"='t' then 1 else 0 end as domestic,
	       geoid10::text as geocode
	from lookup_crimes.crimes
	join
        (
        select entity_id, address_json, geocode
        from events_ucm.events
        join events_ucm.address using (event_id)
        where event_type='address'
        ) as a
        on a.geocode::text = geoid10::text
);

create index on features_cs.crimes(entity_id);
create index on features_cs.crimes(date_col);

-- currently only 6 months
drop table if exists features_cs.crimes;
create temp table crimes_6mo as
(
        select entity_id, --"Date"::date as date_col,
	       update_date-1 as date_col,
	       count(*) as crime_6mo,
	       sum(case when "Primary Type"='THEFT' then 1 else 0 end) as theft_6mo,
               sum(case when "Primary Type"='NARCOTICS' then 1 else 0 end) as narcotics_6mo,
	       sum(case when "Arrest"='t' then 1 else 0 end) as arrest_6mo,
               sum(case when "Domestic"='t' then 1 else 0 end) as domestic_6mo
        from lookup_crimes.crimes
        join
	(
        select entity_id, address_json, geocode, e.update_date
        from events_ucm.events e
        join events_ucm.address a using (event_id)
        where event_type='address'
        ) as a
	on a.geocode::text = geoid10::text
	where "Date"::date > update_date-1-interval'6months' and "Date"::date < update_date-1
	group by 1, 2
);
create temp table crimes_1yr as
(
        select entity_id, --"Date"::date as date_col,
	       update_date-1 as date_col,
	       count(*) as crime_1yr,
	       sum(case when "Primary Type"='THEFT' then 1 else 0 end) as theft_1yr,
               sum(case when "Primary Type"='NARCOTICS' then 1 else 0 end) as narcotics_1yr,
	       sum(case when "Arrest"='t' then 1 else 0 end) as arrest_1yr,
               sum(case when "Domestic"='t' then 1 else 0 end) as domestic_1yr
        from lookup_crimes.crimes
        join
	(
        select entity_id, address_json, geocode, e.update_date
        from events_ucm.events e
        join events_ucm.address a using (event_id)
        where event_type='address'
        ) as a
	on a.geocode::text = geoid10::text
	where "Date"::date > update_date-1-interval'1year' and "Date"::date < update_date-1
	group by 1, 2
);
create temp table crimes_2yr as
(
        select entity_id, --"Date"::date as date_col,
	       update_date-1 as date_col,
	       count(*) as crime_2yr,
	       sum(case when "Primary Type"='THEFT' then 1 else 0 end) as theft_2yr,
               sum(case when "Primary Type"='NARCOTICS' then 1 else 0 end) as narcotics_2yr,
	       sum(case when "Arrest"='t' then 1 else 0 end) as arrest_2yr,
               sum(case when "Domestic"='t' then 1 else 0 end) as domestic_2yr
        from lookup_crimes.crimes
        join
	(
        select entity_id, address_json, geocode, e.update_date
        from events_ucm.events e
        join events_ucm.address a using (event_id)
        where event_type='address'
        ) as a
	on a.geocode::text = geoid10::text
	where "Date"::date > update_date-1-interval'2years' and "Date"::date < update_date-1
	group by 1, 2
);
create table features_cs.crimes as
(
	select *
	from crimes_6mo
	join crimes_1yr using (entity_id, date_col)
	join crimes_2yr using (entity_id, date_col)
);
create index on features_cs.crimes(entity_id);
create index on features_cs.crimes(date_col);
