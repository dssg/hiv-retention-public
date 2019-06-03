# Explanation of sections in config file. 
To modify/create your own experiments, you can use any of the config files in this folder and modify as needed. 
Do not change the indentation. Do not change the headers (characters before a colon). Values that can 
be changed are enclosed in single quotes.

## `config_version: 'v6'`
Version of the config that triage expects; Do not change this without consulting the triage docs.

## `model_comment: 'xxxx'`
Comment attached to all the models run in this experiment. Choose a comment that is clear and concise.

## `user_metadata:`
Metadata for experiment. Choose values that are clear.

## `temporal_config:`
This section deals with when the modeling starts, ends and how data is split. 
This section will be modified based on how your set up changes. For example, if you want models to be updated
every 3 months, change the value of `model_update_frequency` to `3months`

### `feature_start_time: '2041-01-01'`
Earliest time at which to start computing features. Any data before this date will be ignored. 
For example, with the above value, if an individual has a lab test on 2040-12-01 and 2041-02-01, the first test will not
be included in modeling.

### `feature_end_time: '2047-01-02'`
The last time point for which a feature is computed.

### `label_start_time: '2041-01-01'`
The first time point for computing a label/outcome.

### `label_end_time: '2047-01-02'`
The last time point for computing a label/outcome.

### `model_update_frequency: '1months'`
The frequency of retraining a model. Typically this will match your frequency of obtaining new data and rerunning all the models. 
For instance, if you get new data once a year, this value should be `1year`.

### `training_label_timespans: ['6months']` or `test_label_timespans: ['6months']`
The time for which a label is relevant. For instance, for a label of "whether an individual accessed care in 6 months", 
the timespan is 6 months while for a label of "suppression in 12 months", the timespan is 12 months.

### `training_as_of_date_frequencies: '1months'` or test_as_of_date_frequencies: '1months'
Frequency of making predictions in your training (or model building) dataset or your testing (or validation) dataset.
This should match your frequency of making predictions. For instance, if you
make predictions once a quarter, this value should be `3months`. In this study, these will typically be the same value for the 
training and test sets.

###`test_durations: '0d'`
  
###`max_training_histories: '8y'`
The maximum amount of data to include for model building. In this config, we included all previous data that exists. 
If, for example, you want to build a model using only 1 previous year of data, this value should be set to `1year`.


## `label_config:`
The SQL query for the label/outcome. Test this out manually before changing it in the config. 
This label is precomputed as part of the ETL process.

## `cohort_config`
The SQL query looks at the table with the states (`states_cdph`) which include individuals who are alive and then includes
only those who are in the cohort on the prediction date (here, `as_of_date`). The states table is created as part of the 
ETL process.

## `feature_aggregations`
A list of all the feature groups used, along with their source data and the aggregations to compute. And example is provided in 
https://github.com/dssg/hiv-retention-public/tree/master/pipeline_UCM.

## `grid_config`
A list of all the classifier to run, along with their parameters. Most of these come from scikit-learn.

## `feature_group_definition`
The prefixes for the different groups of features. This should match the feature list in feature_aggregations.

## `feature_group_strategies: ['all']`
Train on all features (`all`), with only one group of features or with all but one group of features. This allows us to 
study the value of each feature group.

## `model_group_keys`
Don't change this.

## `scoring`
The metrics to compute and store.
