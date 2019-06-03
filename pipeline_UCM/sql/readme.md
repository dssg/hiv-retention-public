This directory contains SQL scripts for various parts of the experiment.
Unlike typical triage experiments, we run most of the SQL queries before running the experiment. This choice was made since the prediction is being made at an appointment level and thus, effectively every day. Pre-creating labels and features tables prevent thousands of queries during training/test matrix creation.
These are all called by the makefile.

- `make_ucm_labels.sh`: A shell script that called both `access.sql` and `events_and_states.sql` in order to create the cohort table and the corresponding labels.
- `features/*`: Each of these scripts creates one type of feature
