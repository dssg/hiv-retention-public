import yaml
import numpy as np
import pandas as pd
import datetime

from sqlalchemy.sql import text
from sqlalchemy import create_engine

postgres_config = './luigi.yaml'

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


engine = get_db_conn(postgres_config)
connection = engine.connect()

crime_file = "/group/dsapp-lab/lookups/chicago_crime_data/crimes_matched.csv"
d = pd.read_csv(crime_file)
print("read data")
#d['datetime'] = pd.to_datetime(d.Date, infer_datetime_format=True)
#print ("converted dates")

d.to_sql('crimes', connection, schema='lookup_crimes', if_exists='replace')
