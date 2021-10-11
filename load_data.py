import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
import psycopg2 
import io


df = pd.read_json("starlink_historical_data.json")

# We're trying to load just four relevant columns:
# id, spacetrack, longitude, latitude

#print(df.columns)
#print(df.head(1))

df1 = df[['id','spaceTrack', 'longitude', 'latitude']]
#print(df1)

#,'spaceTrack','longitude','latitude'
#print(df.loc[['id']].head(1))
#df.loc()
df1.to_sql('starlink')

engine = create_engine('postgresql+psycopg2://postgres:postgres@0.0.0.0:5432/postgres')
engine.execute
#engine.connect
#engine.

df1.head(0).to_sql('starlink', engine, if_exists='replace',index=False) #drops old table and creates new empty table

conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df1.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'starlink', null="") # null values become ''
conn.commit()