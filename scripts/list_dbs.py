from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json
import os
# from credentials.credential_file import read_json_file


# # Credential Files
# file_path = 'dags/credentials/credentials.json'

# # Reading credentials
# credentials = read_json_file(file_path)


credentials = {"user_name":"postgres","password":3523,"ip_connection":"192.168.0.142","port":5432,"db":"postgres"}
# Setting credentials
user = credentials['user_name']
password = credentials['password']
ip_connection = credentials['ip_connection']
port = credentials['port']
db = credentials['db']


# {"user_name":"postgres","password":3523,"ip_connection":"192.168.144.1","port":5432,"db":"postgres"}


# Function due to take existing db names and save in a json file
def list_dbs_name():
    file_path = 'data_out/db_names.json'
    dict_to_json = {}
    iterator = 1
    engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')
    with engine.connect() as conn:
        query = text("SELECT datname FROM pg_database")
        result = conn.execute(query)
        db_names = [row['datname'] for row in result]
        for name in db_names:
            if not name.startswith("template"):
                dict_to_json[f'db_{iterator}'] = name
                iterator += 1


        with open(file_path, 'w') as f:
            json.dump(dict_to_json, f)


list_dbs_name()
    

