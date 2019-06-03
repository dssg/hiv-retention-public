# ETL Pipeline for UChicago HIV Clinic

This directory contains the files used during ETL.
It is coordinated by the makefile, which is expected to be run from the
current directory.

The raw data is first transformed into a common schema that is common across
different study sites. Features/Labels are then created from this
intermediate data.

## Load Raw Data

Raw data from the UChicago Clinic is in the form of CSVs and can be loaded into the raw schema with `etl.py` in the
etl directory.
```
python etl.py inventory.yaml db_config.yaml
```
where db_config.yaml contains the parammeters for connection to the database.

## Initial database setup

The intermediate staging data is stored in a common schema.
The common schema consists of 3 schemas:
- patient information (e.g. demographics)
- events that occur for the patient (e.g. lab)
- lookups (e.g. lab codes)
The assumption is that patient related information will be static after the initial data dump but the events
can be continually updated and added to.
We do not use the IDs provided by the partners, but generate our own, unique entity_id for every individual. This entity_id ties tables together.

The schema can be deleted by
```
make clean_ucm
```
The tables are then created with
```
make create_ucm_tables
```


## Converting raw data into intermediate data
The raw data can be processed into the intermediate stage using the makefile.
Based on the data to be process, the call is
```
make etl_ucm_patients
make etl_ucm_events
```

## Additional data (crime)
Crime data is downloaded from the Chicago Data Portal (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2/data).
This can be loaded into the database using ```make crime```

## Additional data (census)
Use the [acs2ppgsql](https://github.com/dssg/acs2pgsql) tool to load ACS 5-year data for Illinois into the database.

## Label Creation
There are several labels that are studied in this project:
1. Retention in Care (HRSA HAB Definition used by CDC): Does the individual have 2 appointments that are at least 90 days apart within the 365 days after the prediction date.
2. Access to Care/Receipt of Care: Does the individual have 1 appointment in the next 6 (or 12 or 18) months after the prediction date
3. Viral Suppression: Is the individual virally suppressed in the next 6 (or 12 or 18) months after the prediction date? Implicitly, this checks whether there is a lab test AND that test indicated viral suppression (viral load <= 200).

Note that for all of these labels, the positive class is the *LACK* of retention/access/suppression.

```
cd pipeline_UCM
make labels
```
This creates the label for the HRSA HAB definition of retention and access to care. This will also create the states table for use by Triage.

## Feature Creation
For ease of computation in the experiment runs, we create staging feature tables in the database. The features used in the modeling are then various aggregations of these raw features.

### ICD Codes
Rather than using the ICD diagnosis code directly, we used the category the [AHRQ rollup](https://www.hcup-us.ahrq.gov/tools_software.jsp). ICD9 information came from [here](https://www.hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp#download) and the zip file is [here](https://www.hcup-us.ahrq.gov/toolssoftware/ccs/Single_Level_CCS_2015.zip)

To create all the features in the database:
```
cd pipeline_UCM
make features
```

# Experimental Setup
The configuration for the experiments are done via the [configuration files](https://github.com/dssg/hiv-retention-private/tree/master/pipeline_UCM/configs). These files describe the following features of the experiments:

### What is the temporal structure of your problem? (`temporal_config`) 

This section defines configuration for temporal cross-validation including feature start and end date, label start and end date, frequency of model update, training, and testing as-of-dates, and timespan of training and testing label. In our case, we set the model update frequency, training/test label timespan and training/test label frequency to 6, 12, or 18 months depending on the label definition. Results in the paper are with access at 6 months and retention at 12 months. 
This yields the following temporal splits (red is training, blue is testing):

<p align="center">
  <img src="https://github.com/dssg/hiv-retention-public/blob/master/pipeline_UCM/time_splits.png">
</p>

### Who are the entities of interest? (`cohort_config`)

Contains a SQL query for the table that defines the cohort for a given as of date. This table was created as part of the label creation process. In our case, the cohort is all unique HIV+ patients who are over the age of 18 and are living in Chicago.

### What do we want to predict? (`label_config`) 

Contains a SQL query that define the label for each patient in the cohort. In this project, we look at 3 different labels across 3 time horizons. This table was also created as part of the label creation process. 

### What features do we want? (`feature_aggregations`)
Defines feature groups (`prefix` in config file) and individual features for experiment. For each feature group, a SQL query is provided that defines the `from obj`, a SQL object from which the individual features are calculated. For each individual feature, the aggregation (eg. max, avg) and strategy for imputing missing values is defined. For each feature group, a list of time intervals for aggregation is defined. These time intervals define how far back from the as-of-date we look to calculate features (eg. the average viral load of an individual over the past 3 years). 

### Which models do you you want to train? (`grid_config`) 
Defines the set of classifiers and hyperparameters that the experiment will search over.

### Which metrics do you want to use to evaluate model performance? (`scoring`)
Defines the set of metrics that will be calculated for training and testing. Includes set of metrics (eg. 'precision@') and thresholds (eg. 10%). 

### Feature Example

Below configuration file feature entry would create the following features:
- `vl_entity_id_3years_magnitude_change_avg`: The average magnitude change (current VL / previous VL) for a patient for all the viral load results in the past three years.
- `vl_entity_id_3years_magnitude_change_avg_imp`: Whether the above value had to be imputed (i.e. there is no viral load test in the past three years)
- `vl_entity_id_6months_magnitude_change_avg`: The average magnitude change (current VL / previous VL) for a patient for all the viral load results in the past six months.
- `vl_entity_id_6months_magnitude_change_avg_imp`: Whether the above value had to be imputed (i.e. there is no viral load test in the past six months)
- `vl_entity_id_all_magnitude_change_avg`: The average magnitude change (current VL / previous VL) for a patient for all their previous viral load results.
- `vl_entity_id_all_magnitude_change_avg_imp`: Whether the above value had to be imputed (i.e. this is the first visit to this clinic)
 
```
prefix: 'vl'
    from_obj: features_cs.viral_load
    knowledge_date_column: 'date_col'
    aggregates_imputation:
      all:
        type: 'zero'
    aggregates:
      -
        quantity: 'virally_supressed'
        metrics: [ 'max', 'sum']
      -
        quantity: 'magnitude_change'
        metrics: ['avg']
    intervals: ['all', '6months', '3years']
```

## Index of Experiments
- `baseline_access.yaml`: Configuration file for the baseline models that mimic expert rules.
- `ucm_triage3_access.yml`: Main modeling run including patient features and a large grid config for the outcome of access to care in 6 months. 
- `ucm_triage3_discrete_features.yml`: Main modeling run including patient features and a large grid config for the outcome of retention in care.
