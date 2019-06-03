# HIV Retention

## Goal

*Identify HIV+ Patients who are at risk of dropping out of clinical care*

Retaining individuals living with HIV in medical care is critical to reduce HIV transmission. Individuals retained in care are less likely to progress to AIDS, transmit the virus to others, and are more likely to have longer lifespans than their unretained counterparts; yet maintaining quarterly appointments and daily medication for a lifetime is exceedingly difficult.

In this work, we focus on the problem of prioritizing individuals for retention interventions based on their likelihood of dropping out of care.
We have developed a predictive model that provides a risk score for whether a patient will be retained in care at the time of the patient's doctor visit. Predictors from the model include data from the electronic medical record (e.g. lab values), demographics, and zip-code level census and crime information. This risk score and additional information extracted from the machine learning model is then used by the clinic to assign personalized interventions to the patient to increase their retention likelihood. This methodology has two approaches. In partnership with the UChicago HIV Clinic, we have developed a point-of-care model that will output a score at the time of a patient's appointment. In partnership with the Chicago Department of Public Health, we have developed a batch processing model that ouptuts risk scores for HIV+ persons at-risk of dropping out of care that can be run monthly.

## Partners
- UChicago HIV Care Clinic (UCM)
- Chicago Department of Public Health (CDPH)

## Requirements and Installation
- Linux/Bash Terminal (to run the scripts)
- Python 3.6.5
- PostgreSQL 9.6.5
- (Recommend Miniconda 3.7)
- Triage 3.0


To create a running setup (called an environment) with all the necessary tools installed (including underlying C libraries and python requirements):
```
conda hivenv create -f environment.yml
conda activate hivenv
```
Our environment here is called 'hivenv'

To allow programmatic access to the database, create an environment variables as:
```
export DBURL="postgres://your_username:your_password@url_to_database:xxxx/database_name"
```
where xxxx is the port number. You can also add this to your bash profile so it is available by default when you
access the terminal. To add it to your bash profile:
```
echo "export DBURL="postgres://your_username:your_password@url_to_database:xxxx/database_name" > ~/.bashrc
sh ~/.bashrc
```
The database can then be accessed using `psql`or `SQLAlchemy` using the connection string.



## Cohort
**UCM**: The cohort under study are patients of the UCM HIV Clinic who have had at least 1 appointment from Jan 2008 - Jul 2016.
Predictions are made at the time of appointment so the cohort for any train/test matrix are those individuals with
appointments in that time period.

**CDPH**: The cohort under study are persons living with HIV in the time period from 2010-2016 (TODO: check dates).
Predictions are made monthly on all individuals who have had an HIV-related lab test (viral load, CD4 count or HIV genotype) in year before the prediction date.

### Code
The cohort are stored through a table  called the states table (see specific UCM and CDPH README for more details).
This table is created as part of the label creation process during etl (see [description](etl/README.md#label-creation)).
In the configuration files for the experiment, this is detailed under the 'cohort_config' heading.

## ETL:
These files describes the processing and method to load raw data and convert it into features and labels for
- [UCM](./pipeline_UCM/README.md)
- [CDPH](./pipeline_CDPH/README.md)

## Run Experiments
The main script to run the experiment is in pipeline_UCM/run.py (correspondingly, pipeline_CDPH/run.py).
In the run.py script the following variables need to be set:
- `configfile`: path to configuration file
- `dburl`: path to dburl connection string(typically can use os.environ['DBURL'] to grab DBURL shell variable)
- `project_path`: path to where training and test matrices, and models are stored.
```
python run_models.py -c <config_file> -p <path_to_store_models>
```


We use triage, [triage](https://github.com/dssg/triage), to run and evaluate the models.
This config file specifies everything needed to run the experiment including:
- the cohort
- the date range under study
- the labels (or outcomes)
- the features used
- the models (and corresponding hyperparameters) to run
- the metrics to store (e.g. precision@k, recall@k)

For more details about the configuration file and experimental set up see [UCM](./pipeline_UCM/README.md) and [CDPH](./pipeline_CDPH/README.md).

All the config files are stored in pipeline_UCM/configs (or pipeline_CDPH/configs).
The default config files to use are:
- **UCM**: ./pipeline_UCM/configs/ucm_triage3_discrete_features.yml
- **CDPH**: ./pipeline_CDPH/configs/cdph_triage3_test_grid_6months.yml


## Analysis of results

The results of the modeling are stored in a PostgreSQL database whose configs are specified earlier.
Model selection was done using [audition](https://github.com/dssg/triage/tree/master/src/triage/component/audition).


## Contributors

- Adolfo De Unanue (adolfo@uchicago.edu)
- Avishek Kumar (avishekkumar@uchicago.edu)
- Arthi Ramachandran (arthi@uchicago.edu)
- Hannes Koenig (koenigh@uchicago.edu)
- Adolfo De Unanue (adolfo@uchicago.edu)
- Joseph  Walsh (jtwalsh@uchicago.edu)
- Christina Sung (csung1@uchicago.edu)
