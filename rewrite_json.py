import json
import os
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
import psycopg2 

ENGINE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'


def doIt():
    print("start")
    data = ""
    with open("starlink_historical_data.json","r+") as o:
        data = "".join(o.readlines())
    json_input = json.loads(data)

    with open("new_json_output2.csv","w") as output:
        output.write("creation_date,id,longitude,latitude\n")
        for i in range(0,len(json_input)):
            ip = json_input[i]
            if not (ip['longitude'] is None or ip['latitude'] is None):
                #y = 1 # do nothing for the if.
                #print(f"skipped id: {ip['id']} and time: {ip['spaceTrack']['CREATION_DATE']}")
            # else: 
                output.write(f"{ip['spaceTrack']['CREATION_DATE']},{ip['id']},{ip['longitude']},{ip['latitude']}\n")
    
def loadCSV():
    data_frame = pd.read_csv("new_json_output2.csv",header=[0])
    print(data_frame.head(1))
    
    engine = create_engine(ENGINE_CONN_STR)
    data_frame.head(0).to_sql('source_starlink', engine, if_exists='replace',index=False) #drops old table and creates new empty table
    conn = engine.raw_connection()
    cur = conn.cursor()
    print('the middle')
    output = io.StringIO()
    data_frame.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'source_starlink', null="") # null values become ''
    conn.commit()
    print('the end')

def runSetup():
    engine = create_engine(ENGINE_CONN_STR)
    with open("sql_solution.sql","r") as sql:
        sql_command = ""
        for string_line in sql.readlines():
            sql_command += string_line
            if ";" in string_line:
                print("executing command:")
                print(sql_command)
                engine.execute(sql_command)
                sql_command = ""
    

if __name__ == "__main__":
    doIt()
    loadCSV()
    runSetup()