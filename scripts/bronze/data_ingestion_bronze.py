'''
This script automates data ingestion into the bronze layer from source folders - crm and erm.
Each file is loaded into the dataframe, the script truncates then existing table and ingests the data into the appropriate database table.
Warning -It is assumed that the datasets folder is in the same directory as the script.

'''

import pandas as pd
from sqlalchemy import create_engine,text
from dotenv import load_dotenv
import os
from  pathlib import Path

load_dotenv()

user = os.environ["MYSQL_USER"]
password = os.environ["MYSQL_PASSWORD"] 
host = os.environ["HOST"]
dbname = os.environ["DATABASE"]

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{dbname}')

dataset_dir = Path("datasets")
source_dirs = [ d for d in dataset_dir.iterdir() if d.is_dir()]

try:
    for dir in source_dirs:
        source = dir.name.split("_")[1]
        for file in dir.iterdir():
            df = pd.read_csv(file)
            table = f"bronze_{source}_{file.stem.lower()}"
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
                df.to_sql(name=table, con=engine, if_exists="append", index=False)
            print(f"{file.name} ingested into table '{table}' successfully.")
            
except Exception as e:
    print(f"Error ingesting {file.name}: {e}")
