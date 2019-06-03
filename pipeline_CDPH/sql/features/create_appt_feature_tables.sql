drop table if exists features_cdph.appt;
create table features_cdph.appt as
       (
	select distinct entity_id,
	       update_date as date_col,
	       1 as completed,
	       (
                update_date - LAG(update_date) OVER (
                 PARTITION BY entity_id ORDER BY update_date)
               )::INT as days_between_appts
        from events_ucm.events
	where event_type = 'lab'
       );
create index on features_cdph.appt(entity_id);
create index on features_cdph.appt(date_col);
