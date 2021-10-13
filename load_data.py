import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
import psycopg2 
import json




# We're trying to load just four relevant columns:
# id, spacetrack, longitude, latitude
#ENGINE_CONN_STR = 'postgresql+psycopg2://postgres@0.0.0.0:5432/postgres'
ENGINE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
#ENGINE_CONN_STR = 'postgresql://postgres:postgres@0.0.0.0:5432/postgres'
TEST_DB_CONN_STR = 'postgresql://postgres:postgres@0.0.0.0:5432/test'
def testConn():
    e = create_engine(ENGINE_CONN_STR)
    e.execute("drop database if exists test")
    e.execute("create database if not exists test with owner = postgres")
    print('created connection')
    
def testDBConn():
    e = create_engine(ENGINE_CONN_STR)
    db = e.connect()
    x = db.execute("select * from information_schema.tables")
    for x1 in x:
        print(type(x1))
        print(x1)
    print('done connecting')
    #with (e.connect()) as connection:
    #    print('here')
    #    print(connection.execute("select * from information_schema.tables"))

#print(df.columns)
#print(df.head(1))
def doWork():
    df = pd.read_json("starlink_historical_data.json")
    df1 = df[['id','spaceTrack', 'longitude', 'latitude']]
    print(df1.size)
    for x in range(0,df1.size):
        r = df1[x]
        print(dir(r))
        print(type(r))
        #r['creation_date'] = json.loads(r['spaceTrack'])['CREATION_DATE']
    print(df1.head(1))

    #,'spaceTrack','longitude','latitude'
    #print(df.loc[['id']].head(1))
    #df.loc()
    #df1.to_sql('starlink')
    
    #df1.to_sql("starlink","spacex")

    #engine = create_engine()
    #print(dir(engine))
    #x = engine.execute("show tables")
    #print(dir(x))

def doWorkTwo():
    df = pd.read_json("starlink_historical_data.json")
    df1 = df[['id','spaceTrack', 'longitude', 'latitude']]
    print(df1.head(1))
    print('the start')
    engine = create_engine(ENGINE_CONN_STR)
    df1.head(0).to_sql('starlink', engine, if_exists='replace',index=False) #drops old table and creates new empty table
    conn = engine.raw_connection()
    cur = conn.cursor()
    print('the middle')
    output = io.StringIO()
    df1.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'starlink', null="") # null values become ''
    conn.commit()
    print('the end')

if __name__ == "__main__":
    #doWork
    print('main')
    #testConn()
    #testDBConn()
    doWork()
    #doWorkTwo()


#engine.connect
#engine.

"""
df1.head(0).to_sql('starlink', engine, if_exists='replace',index=False) #drops old table and creates new empty table

conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df1.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'starlink', null="") # null values become ''
conn.commit()
"""