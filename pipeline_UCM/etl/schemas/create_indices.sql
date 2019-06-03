CREATE INDEX ON patients_ucm.main (patient_id);
CREATE INDEX ON patients_ucm.names (entity_id);
CREATE INDEX ON patients_ucm.demographics (entity_id);



/*
 * Indices on the events table
 */
/* Basic indices */
DROP INDEX events_ucm.idx_event_type;
CREATE INDEX CONCURRENTLY idx_event_type ON events_ucm.events (event_type);
DROP INDEX events_ucm.idx_entity_id;
CREATE INDEX CONCURRENTLY idx_entity_id ON events_ucm.events (entity_id);
DROP INDEX events_ucm.idx_batch_id;
CREATE INDEX CONCURRENTLY idx_batch_id ON events_ucm.events (batch_id);
DROP INDEX events_ucm.idx_update_date;
CREATE INDEX CONCURRENTLY idx_update_date ON events_ucm.events (update_date);
DROP INDEX events_ucm.idx_visit_date;
CREATE INDEX CONCURRENTLY idx_visit_date ON events_ucm.events (visit_date);


/* Indices specific to the queries */
-- Visits
DROP INDEX events_ucm.idx_event_visit;
CREATE INDEX CONCURRENTLY idx_event_visit ON events_ucm.events (event_type) WHERE (event_type = 'visit');
DROP INDEX events_ucm.idx_event_id_visit;
CREATE INDEX CONCURRENTLY idx_event_id_visit ON events_ucm.events(event_type, id_provider_flag) WHERE (event_type = 'visit' AND id_provider_flag = 1);

DROP INDEX events_ucm.idx_event_id_visit_completed;
CREATE INDEX CONCURRENTLY idx_event_id_visit_completed
       ON events_ucm.events(event_type, id_provider_flag, visit_status_id) WHERE (event_type = 'visit' AND id_provider_flag = 1 AND visit_status_id = 5);
DROP INDEX events_ucm.idx_event_visit_completed;
CREATE INDEX CONCURRENTLY idx_event_visit_completed
       ON events_ucm.events(event_type, visit_status_id) WHERE (event_type = 'visit' AND visit_status_id = 5);

DROP INDEX events_ucm.idx_event_id_visit_cancelled;
CREATE INDEX CONCURRENTLY idx_event_id_visit_cancelled
       ON events_ucm.events(event_type, id_provider_flag, visit_status_id) WHERE (event_type = 'visit' AND id_provider_flag = 1 AND visit_status_id = 7);
DROP INDEX events_ucm.idx_event_visit_cancelled;
CREATE INDEX CONCURRENTLY idx_event_visit_cancelled
       ON events_ucm.events(event_type, visit_status_id) WHERE (event_type = 'visit' AND visit_status_id = 7);

-- Gender
DROP INDEX events_ucm.idx_event_gender;
CREATE INDEX CONCURRENTLY idx_event_gender ON events_ucm.events (event_type) WHERE (event_type = 'gender');
DROP INDEX events_ucm.idx_gender;
CREATE INDEX CONCURRENTLY idx_gender ON events_ucm.gender (event_id);

