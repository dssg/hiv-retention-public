#!/bin/bash


psql -f cdph_valid_labs.sql;
# add the retention label definitions file
psql -f cdph_access_events.sql;
psql -f cdph_suppression_events.sql;
psql -f cdph_states.sql;
