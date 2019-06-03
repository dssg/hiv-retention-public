#!/bin/bash

psql -f events_and_states.sql
psql -f access.sql
