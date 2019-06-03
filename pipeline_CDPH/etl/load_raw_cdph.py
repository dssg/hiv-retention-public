import sys
import yaml

from sqlalchemy.sql import text
from sqlalchemy import create_engine


def get_db_conn(postgres_config):
    """Get db connection (bad code)."""
    with open(postgres_config, 'r') as f:
        config = yaml.load(f)
    dbtype = 'postgres'

    # previously was: user = config['postgres']['user']                                                                                                       
    user = config['user']
    host = config['host']
    port = config['port']
    db = config['database']
    passcode = config['password']
    url = '{}://{}:{}@{}:{}/{}'.format(dbtype,
                                       user,
                                       passcode,
                                       host,
                                       port,
                                       db)
    conn = create_engine(url)
    return conn

dir_path_cdph = '/group/dsapp-lab/CDPH/'
dir_path_cdph1 = '/group/dsapp-lab/cdph_drive1/'
dir_path_cdph2 = '/group/dsapp-lab/cdph_drive2/'


def load_cdph_raw(postgres_config):
    """ Loads the raw CDPH data into the database
        and converts from sas 
    In:
        - postgres_config: path to postgres_config              
    """
    engine = get_db_conn(postgres_config)
    connection = engine.connect()

    # create the raw schema
    query = """DROP SCHEMA IF EXISTS raw_cdph CASCADE;"""
    connection.execute(query)
    query = """CREATE SCHEMA raw_cdph;"""
    connection.execute(query)

    #data['/group/dsapp-lab/cdph_drive2/country_code.sas7bdat'] = pd.read_sas('/group/dsapp-lab/cdph_drive2/country_code.sas7bdat')
    for filename, df in data.items():
    # ignore the first data dump
        if "CDPH" in filename:
            continue;
        if "Copy" in filename:
            continue;
        table_name = filename.split("/")[4].split('.')[0]
        # if table already in DB and is not empty, skip
        query = """SELECT EXISTS (
           SELECT 1 
           FROM   pg_tables
           WHERE  schemaname = 'raw_cdph'
           AND    tablename = '{}'
           );""".format(table_name)
        d = query_db(query, connection)
        if d['exists'].any():        
            query = """SELECT CASE 
            WHEN EXISTS (SELECT * FROM raw_cdph.{} LIMIT 1) THEN 1
            ELSE 0 
            END;""".format(table_name)
            d = query_db(query, connection)
        if d['case'].any():
            continue
        print(filename + "   " + table_name)
    
    try:
        df.shape
    except:
        print ("Table "+table_name + "could not be read")
        continue
    str_df = df.select_dtypes([np.object])
    str_df = str_df.stack().str.decode('utf-8', "ignore").unstack()
    for col in str_df:
        df[col] = str_df[col]
    df.head()
    print("done converting")
    df.to_sql(table_name, engine, schema = 'raw_cdph', if_exists='replace')

    # add the eHARS race lookup table into the database
    # this was converted to an individual csv from the whole data dictionary
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/race.csv", na_values=["NULL"])
    df_data.to_sql("race", engine, schema = 'raw_cdph', if_exists='replace')

    # add the eHARS ethnicity table into the database
    # this was converted to an individual csv from the whole data dictionary
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/ethnicity.csv", na_values=["NULL"])
    df_data.to_sql("ethnicity", engine, schema = 'raw_cdph', if_exists='replace')

    # put the eHARS facility types table into the database
    # this was converted to an individual csv from the whole data dictionary
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/facility_type.csv", na_values=["NULL"])
    df_data.to_sql("facility_type", engine, schema = 'raw_cdph', if_exists='replace')

    # put the eHARS lab types table into the database
    # this was converted to an individual csv from the whole data dictionary
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/lab_type.csv", na_values=["NULL"])
    df_data.to_sql("lab_type", engine, schema = 'raw_cdph', if_exists='replace')

    # put the eHARS lab types table into the database
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/genders.csv", na_values=["NULL"])
    df_data.to_sql("genders", engine, schema = 'raw_cdph', if_exists='replace')

    # put the eHARS address types table into the database
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/address_types.csv", na_values=["NULL"])
    df_data.to_sql("address_type", engine, schema = 'lookup_cdph', if_exists='replace', index=False)

    # put the eHARS race table into the database
    # this was converted to an individual csv from the whole data dictionary
    # one of the lab test codes was missing in the eHARS data dictionary (EC-029)
    # manually added it from /group/dsapp-lab/raw/cdph_drive3/HICSB_LOINC MAPPINGS_v4.9_draft.xslx
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/lab_test_code.csv", na_values=["NULL"])
    df_data.to_sql("lab_test_code", engine, schema = 'raw_cdph', if_exists='replace')
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/lab_VL_test_type.csv", na_values=["NULL"])
    df_data.to_sql("lab_vl_test_type", engine, schema = 'raw_cdph', if_exists='replace')
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/lab_result_units.csv", na_values=["NULL"])
    df_data.to_sql("lab_result_units", engine, schema = 'raw_cdph', if_exists='replace')
    df_data = pd.read_csv("/group/dsapp-lab/lookups/eHARS_lookups/oi_cd.csv", na_values=["NULL"])
    df_data.to_sql("oi_diagnosis_cd", engine, schema = 'raw_cdph', if_exists='replace')

    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("Need argument for postgres_config.")
    load_cdph_raw(sys.argv[1])
