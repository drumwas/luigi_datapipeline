import json
import luigi
import psycopg2
import requests
from airtable import Airtable
import luigi.contrib.postgres
from sqlalchemy import create_engine
import pandas as pd

Port = '#####'
db = '#####'
username = '#####'
password = '#####'
host='#####'
table='######'



class loadAirtableData(luigi.Task):
    def require(self):
        return None
    def output(self):
        return luigi.contrib.postgres.PostgresTarget(host=host, database=db, user=username,password=password, table=table)   
 
    def run(self):
        base_id='#####'
        api_key='#####'
        table_name='Cases'
        at = Airtable(base_id,table_name,api_key)
        records=at.get_all()
        df = pd.DataFrame.from_records((r['fields'] for r in records))
        conn = psycopg2.connect(host=host,dbname=db,user=username,password= password)
        engine = create_engine('postgresql://#######')
        df.to_sql('dispatch_data', engine,index=False, if_exists='replace')
    
          

class postgresqldata(luigi.Task):
    def requires(self):
        return loadAirtableData()
    def output(self):
        return luigi.LocalTarget('mergedData.csv')
    def run():
        conn = psycopg2.connect(host=host,dbname=db,user=username,password= password)
        # conn.autocommit = True
        cur=conn.cursor()
        cur.execute("SELECT * FROM dispatch_data FULL OUTER JOIN Cases ON dispatch_data.hospital_delivered_to=Cases.facility_name")
        conn.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
    luigi.run()





