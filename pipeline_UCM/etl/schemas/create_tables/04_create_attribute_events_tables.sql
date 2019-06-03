DROP TABLE IF EXISTS events_ucm.main CASCADE;
CREATE TABLE events_ucm.main (
       event_id	   SERIAL PRIMARY KEY,
       entity_id   INT REFERENCES patients_ucm.main (entity_id)
);

DROP TABLE IF EXISTS events_ucm.gender;
CREATE TABLE events_ucm.gender (
       event_id	   INT REFERENCES events_ucm.events (event_id),
       gender_id   gender_enum
);

DROP TABLE IF EXISTS events_ucm.address;
CREATE TABLE events_ucm.address (
       event_id		    INT REFERENCES events_ucm.events (event_id),
       update_date     	    DATE,
       address_type_id	    INT REFERENCES lookup_ucm.address_type (address_type_id),
       address_json	    TEXT,
       address_seq	    INT,
       geocode		    TEXT
);
