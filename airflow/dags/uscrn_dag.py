import requests
import pandas as pd 
import numpy as np
import os 
import re
import datetime as dt
from yaml import full_load
from collections import deque
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
# Utilities imports: 
# from utils.utils import nanCheck (TO-DO)
# Airflow imports: 
from airflow.decorators import dag, task
# GCP imports: 
from google.cloud import bigquery, storage
from google.oauth2 import service_account

## ---------- GLOBAL VARIABLES ---------- ## 

## Path information
PATH = os.path.abspath(__file__)
DIR_NAME = os.path.dirname(PATH)
PARENT_DIR = os.path.dirname(DIR_NAME)

## Airflow Schedule
INTERVAL = "@once" 
START = dt.datetime.now()  
# ^^ Would not normally do this, but our DAG has no "backfill" aspect and this is fine for a demo

# Data Source URLs
with open(f"{DIR_NAME}/config/sources.yaml", "r") as fp:
  SOURCES = full_load(fp)

## --- Google Cloud --- ## 

# GCP/BigQuery information
with open(f"{DIR_NAME}/config/gcp-config.yaml", "r") as fp:
  gcp_config = full_load(fp)
PROJECT_ID = gcp_config['project-id']
DATASET_ID = gcp_config['dataset-id']
STAGING_TABLE_ID = 'uscrn_staging'
MAIN_TABLE_ID = 'uscrn'


# Credentials -- access from docker-compose.yaml
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(
  key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Create bigquery client
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# Create google cloud storage client
storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = storage_client.bucket(f"{PROJECT_ID}-bucket") # made in first notebook: 1_uscrn_scrape.ipynb

# Download locations  
blob = bucket.blob("locations.csv")
content = blob.download_as_bytes()
locations_df = pd.read_csv(BytesIO(content))


## ---------- SET LOGGING ---------- ## 
### Airflow does log any print statements, but here's an example of how to log directly

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(f'{PARENT_DIR}/logs/uscrn_dag_logs.txt')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


## ---------- DEFINING TASKS ---------- ## 
@task
def check_connection() -> None:
    url = "https://ncei.noaa.gov/"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print(f"Connection to {url} is successful")
        else:
            print(f"Connection to {url} failed with status code {response.status_code}")
    except requests.exceptions.Timeout as e:
        print(f"Connection to {url} timed out: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Connection to {url} failed: {e}")

@task
def check_last_added() -> str: # String representation of datetime
  """Reads/returns latest 'date_added_utc' value from main table"""
  
  query = f"""
  SELECT date_added_utc 
  FROM {DATASET_ID}.{MAIN_TABLE_ID}
  ORDER BY date_added_utc DESC LIMIT 1
  """

  query_job = bq_client.query(query)
  result = query_job.result()

  row = next(result)
  last_added = row['date_added_utc']
  last_added_str = dt.datetime.strftime(last_added, format="%Y-%m-%d %H:%M:%S.%f")

  return last_added_str

@task
def get_new_file_urls(last_added:str, ti=None) -> list: 
  """Check/obtain updates from USCRN updates page"""
  now = dt.datetime.utcnow()
  updates_url = SOURCES['USCRN']['updates'] + "/" + str(now.year)

  last_added = dt.datetime.strptime(last_added,"%Y-%m-%d %H:%M:%S.%f")

  df = pd.read_html(updates_url, skiprows=[1,2])[0]
  df.drop(["Size", "Description"], axis=1, inplace=True)
  df.dropna(inplace=True)
  df['Last modified'] = pd.to_datetime(df['Last modified'])

  df = df[df['Last modified'] > last_added]

  new_file_urls = list(updates_url + "/" + df['Name'])

  return new_file_urls

# @task
# def getUpdates(new_file_urls:list) -> list: 
#   """Scrape data from list of new urls, store and return as list of lists"""

#   # locations = pd.read_csv(f"{DIR_NAME}/data/locations.csv")
#   wbs = set(locations_df['wbanno'])

#   rows = []
#   for url in new_file_urls:
#     # Log the current URL being processed
#     logger.info(f'Processing URL: {url}')
#     # Scrape data from URL
#     response = requests.get(url)
#     soup = BeautifulSoup(response.content,'html.parser')
#     soup_lines = str(soup).strip().split("\n")[3:]
#     ak_rows = [re.split('\s+', line) for line in soup_lines if line[0:5] in wbs] # line[0:5] contains WBANNO code
#     rows.extend(ak_rows)

#   # Log the number of rows extracted
#   logger.info(f'Extracted {len(rows)} rows from URLs')

#   return rows

# @task
# def transform_df(rows:list, ti=None): 
#   """Read rows from getUpdates(), cast to dataframe, transform, write to csv"""
  
#   # Get column headers 
#   columns = list(pd.read_csv(f"{DIR_NAME}/data/column_descriptions.csv")['col_name'])

#   # Get locations
#   locations = pd.read_csv(f"{DIR_NAME}/data/locations.csv")
#   locations = locations[['station_location', 'wbanno']]
#   locations['wbanno'] = locations['wbanno'].astype(int).astype(str) 
#   locations.set_index("wbanno", inplace=True)

#   # Create dataframe
#   df = pd.DataFrame(rows, columns=columns[1:])

#   ## (TO-DO) Check for NaN values from source

  
#   # Merge locations
#   df = df.merge(locations, how="left", left_on="wbanno", right_index=True)

#   ## (TO-DO) Check for NaN values from merge 

#   # Reorder columns 
#   columns = ['station_location'] + list(df.columns)[:-1]
#   df = df[columns]

#   # Change datatypes
#   df = df.apply(pd.to_numeric, errors='ignore')

#   ## (TO-DO) Check for NaN values from type transform 


#   ## ------ If no NaNs will be masked from source, safe to replace missing value designators ------ ## 

#   # Replace missing value designators with NaN
#   df.replace([-99999,-9999], np.nan, inplace=True) 
#   df.replace({'crx_vn':{-9:np.nan}}, inplace=True)
#   df = df.filter(regex="^((?!soil).)*$") # almost all missing values

#   # Create datetime columns
#   df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
#   df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

#   # Drop old date and time columns 
#   df.drop(['utc_date', 'utc_time', 'lst_date', 'lst_time'], axis=1, inplace=True)

#   # Reorder columns 
#   cols = ['station_location','wbanno','crx_vn','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
#   df = df[cols]

#   # Add date-added column (utc)
#   df['date_added_utc'] = dt.datetime.utcnow() 

#   # Write to .csv
#   # XCOM Pull: `update_range` from XCOM created by 'getNewFileUrls'
#   # update_range = ti.xcom_pull(key="update_range", task_id="getNewFileURLs")
#   update_range = ti.xcom_pull(key="update_range", dag_id="uscrn_dag", task_ids="getNewFileURLs")
#   df.to_csv(f"{DIR_NAME}/data/uscrn_updates/{update_range[0]}-{update_range[1]}.csv", index=False)


# @task
# def uploadBQ(ti=None):
#   """Upload latest uscrn_update .csv file to BigQuery"""

#   # Set credentials
#   key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#   credentials = service_account.Credentials.from_service_account_file(
#    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
#   )

#   # Create bigquery client
#   bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

#   # Set schema and job_config
#   schema = [
#     bigquery.SchemaField("station_location", "STRING", mode="REQUIRED"), 
#     bigquery.SchemaField("wbanno", "STRING", mode="REQUIRED"), 
#     bigquery.SchemaField("crx_vn", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("utc_datetime", "DATETIME", mode="REQUIRED"), 
#     bigquery.SchemaField("lst_datetime", "DATETIME", mode="REQUIRED"), 
#     bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"), 
#     bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"), 
#     bigquery.SchemaField("t_calc", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("t_hr_avg", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("t_max", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("t_min", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("p_calc", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad_max", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad_max_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad_min", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("solarad_min_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_type", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_max", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_max_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_min", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("sur_temp_min_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("rh_hr_avg", "FLOAT", mode="NULLABLE"), 
#     bigquery.SchemaField("rh_hr_avg_flag", "STRING", mode="NULLABLE"), 
#     bigquery.SchemaField("date_added_utc", "DATETIME", mode="REQUIRED")
#   ]

#   jc = bigquery.LoadJobConfig(
#     source_format = bigquery.SourceFormat.CSV,
#     skip_leading_rows=1,
#     autodetect=False,
#     schema=schema,
#     create_disposition="CREATE_NEVER",
#     write_disposition="WRITE_APPEND"   
#   )
 
#   # Set target table in BigQuery
#   full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
#   # Read file to dataframe first -- direct loading creating schema issues

#   update_range = ti.xcom_pull(key="update_range", dag_id="uscrn_dag", task_ids="getNewFileURLs")
#   file_path = f"{DIR_NAME}/data/uscrn_updates/{update_range[0]}-{update_range[1]}.csv"
#   df = pd.read_csv(file_path)

#   # Upload 
#   job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=jc)
#   job.result()
  
#   # Log result 
#   print(f"Loaded {df.size} rows and {len(df.columns)} columns")

# @task
# def appendLocal(ti=None):
#   """Append latest uscrn_update .csv file to local copy of uscrn.csv"""

#   update_range = ti.xcom_pull(key="update_range", dag_id="uscrn_dag", task_ids="getNewFileURLs")
#   file_path = f"{DIR_NAME}/data/uscrn_updates/{update_range[0]}-{update_range[1]}.csv"

#   updates_df=pd.read_csv(file_path)
#   updates_df.to_csv(f"{DIR_NAME}/data/uscrn.csv", mode="a", header=False, index=False)

## ---------- DEFINING DAG ---------- ## 
@dag(
   schedule_interval=INTERVAL,
   start_date=START,
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def uscrn_dag():
    
    t1 = check_connection()
    t2 = check_last_added()
    t3 = get_new_file_urls(t1)

    # t3 = getUpdates(t2)
    # t4 = transform_df(t3)
    # t5 = uploadBQ()
    # t6 = appendLocal()

    [t1,t2] >> t3 # >> t4 >> [t5, t6]

dag = uscrn_dag()


