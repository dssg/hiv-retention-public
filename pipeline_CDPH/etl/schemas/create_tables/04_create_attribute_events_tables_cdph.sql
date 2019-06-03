DROP TABLE IF EXISTS events_cdph.main CASCADE;
CREATE TABLE events_cdph.main (
       event_id	   SERIAL PRIMARY KEY,
       entity_id   INT REFERENCES patients_cdph.main (entity_id)
);

DROP TABLE IF EXISTS events_cdph.gender;
CREATE TABLE events_cdph.gender (
       event_id	   INT REFERENCES events_cdph.events (event_id),
       gender_id   gender_enum
);

DROP TABLE IF EXISTS events_cdph.address;
CREATE TABLE events_cdph.address (
       event_id		    INT REFERENCES events_cdph.events (event_id),
       update_date     	    DATE,
       address_type_id	    INT REFERENCES lookup_cdph.address_type (address_type_id),
       address_json	    TEXT,
       address_seq	    INT,
       geocode		    INT
);
