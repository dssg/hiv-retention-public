import yaml
import numpy as np
import pandas as pd
from censusgeocode import CensusGeocode

from sqlalchemy.sql import text
from sqlalchemy import create_engine

postgres_config = '/group/dsapp-lab/luigi.yaml'

print ("hello")

def get_db_conn(postgres_config):
    """
    This is really bad code
    """
    with open(postgres_config, 'r') as f:
        config = yaml.load(f)
    dbtype = 'postgres'
    
    #previously was: user = config['postgres']['user']
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

def query_db(query, conn, params=None):
    """
    Queries DB and returns pandas df.
    """
    if params:
        return pd.read_sql(query, conn, params=params)
    else:        
        return pd.read_sql(query, conn)


engine = get_db_conn(postgres_config)
connection = engine.connect()

query = text('''
                select entity_id, 
                    json_build_object('address1', trim(lower(address_1)), 
                                      'address2', trim(lower(address_1b)),
                                      'city', trim(lower(city_1)),
                                      'state', trim(lower(state_1)),
                                      'zipcode', zipcode_1) as address,
                    enroll_date::date as update_date
                from raw.epic_adress
                join patients_ucm.main 
                    ON mrn = patient_id::int
                join raw.final_mrns USING (mrn)
        ''')
df = query_db(query, connection)
print(df.head())

cg=CensusGeocode()
def getGeocode(x):
    try:
        address = " ".join([str(i)for i in filter(None, x.values())])
        #result = cg.onelineaddress(address)
        #geoid = result[0]['geographies']['Census Tracts'][0]['GEOID']
    except:
        #del x['address2']
        print(address)
        return None
    return geoid


df['geoid'] = df['address'].apply(getGeocode)
df.head()
df.to_sql("address_geoid", engine, schema = 'raw', if_exists='replace')
