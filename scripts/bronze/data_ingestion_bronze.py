import pandas as pd
from sqlalchemy import create_engine,text
from dotenv import load_dotenv
import os
from  pathlib import Path
import logging
import time

load_dotenv()

user = os.environ["MYSQL_USER"]
password = os.environ["MYSQL_PASSWORD"] 
host = os.environ["HOST"]
dbname = os.environ["DATABASE"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler() ]
)


engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{dbname}')

dataset_dir = Path("datasets")
source_dirs = [ d for d in dataset_dir.iterdir() if d.is_dir()]

try:
    bronze_load_start = time.time()
    
    for dir in source_dirs:
        source = dir.name.split("_")[1]
        
        for file in dir.iterdir():
            file_start = time.time()
            try:
                df = pd.read_csv(file)
                if df.empty:
                    logging.warning(f"Skipped {file.name} — empty file")
                    continue

                table = f"bronze_{source}_{file.stem.lower()}"

                # Truncate table
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {table}"))

                # Insert data
                df.to_sql(name=table, con=engine, if_exists="append", index=False)

                elapsed = time.time() - file_start
                logging.info(f"{file.name} loaded into '{table}' in {elapsed:.2f} seconds")

            except Exception as file_err:
                logging.error(f"Error processing file {file.name}: {file_err}")

    total_elapsed = time.time() - bronze_load_start
    logging.info(f"Bronze layer loaded in {total_elapsed:.2f} seconds")

except Exception as e:
    logging.error(f"Error in ingestion loop: {e}")
