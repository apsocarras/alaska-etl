import requests
import pandas as pd 
import numpy as np
import os 
import re
import datetime as dt
from yaml import full_load
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
# Airflow imports: 
from airflow.decorators import dag, task
# GCP imports: 
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
# Utilities imports 
import utils.utils as utils

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

# Download column descriptions  
blob = bucket.blob("column_descriptions.csv")
content = blob.download_as_bytes()
column_descriptions = pd.read_csv(BytesIO(content))


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
def check_domain () -> None:
    """Checks connection to USCRN main domain"""
    utils.check_connection(domain="https://ncei.noaa.gov", logger=logger)


@task
def check_last_added() -> str: # String representation of datetime
  """Reads/returns latest 'date_added' value from main table"""
  
  query = f"""
  SELECT date_added 
  FROM {DATASET_ID}.{MAIN_TABLE_ID}
  ORDER BY date_added DESC LIMIT 1
  """

  query_job = bq_client.query(query)
  result = query_job.result()

  row = next(result)
  last_added = row['date_added']
  last_added_str = dt.datetime.strftime(last_added, format="%Y-%m-%d %H:%M:%S.%f")

  return last_added_str

@task
def get_new_file_urls(last_added:str) -> list: 
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

@task
def get_updates(new_file_urls:list) -> list: 
  """Scrape data from list of new urls, store and return as list of lists"""

  wbanno_codes = set(locations_df['wbanno'])
  rows = []
  for url in new_file_urls:
    # Log the current URL being processed
    logger.info(f'Processing URL: {url}')
    # Scrape data from URL
    result = requests.get(url)
    soup = BeautifulSoup(result.content, "html.parser") 
    soup_lines = str(soup).strip().splitlines()[3:]
    ak_rows = [re.split('\s+', line) for line in soup_lines if line[0:5] in wbanno_codes] 
    rows.extend(ak_rows)

  # Log the number of rows extracted
  logger.info(f'Extracted {len(rows)} rows from URLs')

  return rows

@task
def transform_df(rows:list, ti=None): 
  """Read rows from getUpdates(), cast to dataframe, transform, write to csv"""
  
  # Get column headers: 
  columns = list(column_descriptions['col_name'])
  # For reference: 
  ## columns = ['station_location','wbanno','utc_date','utc_time','lst_date','lst_time','crx_vn','longitude','latitude',
  ##'t_calc','t_hr_avg','t_max','t_min','p_calc','solarad','solarad_flag','solarad_max','solarad_max_flag','solarad_min',
  ##'solarad_min_flag','sur_temp_type','sur_temp','sur_temp_flag','sur_temp_max','sur_temp_max_flag','sur_temp_min',
  ##'sur_temp_min_flag','rh_hr_avg','rh_hr_avg_flag','soil_moisture_5','soil_moisture_10','soil_moisture_20',
  ##'soil_moisture_50','soil_moisture_100','soil_temp_5','soil_temp_10','soil_temp_20','soil_temp_50','soil_temp_100']  

  # Create dataframe from rows 
  df = pd.DataFrame(rows, columns=columns[1:]) # exclude station_location -- have yet to add by joining on wbanno

  # Merge with locations 
  a = locations_df[['station_location','wbanno']]
  a['wbanno'] = a['wbanno'].astype(int).astype(str) # remove trailing decimal
  a.set_index("wbanno", inplace=True)

  df = df.merge(a, how="left", left_on="wbanno", right_index=True)

  # Move location column to front  
  columns = ['station_location'] + list(df.columns)[:-1]
  df = df[columns]

  # Change datatypes
  df = df.apply(pd.to_numeric, errors='ignore')

  # Replace missing value designators with NaN
  df.replace([-99999,-9999], np.nan, inplace=True) 
  df.replace({'crx_vn':{-9:np.nan}}, inplace=True)
  df = df.filter(regex="^((?!soil).)*$") # soil columns have almost all missing values

  # Create datetime columns
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # Drop old date and time columns 
  df.drop(['utc_date', 'utc_time', 'lst_date', 'lst_time'], axis=1, inplace=True)

  # Reorder columns after drop
  cols = ['station_location','wbanno','crx_vn','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # Check for duplicates in key columns: 
  duplicates = df.duplicated(subset=["station_location", "utc_datetime", "lst_datetime"], keep=False)
  duplicate_rows = df[duplicates]
  if not duplicate_rows.empty:
    logger.warning(f"Warning: {len(duplicate_rows)} rows have duplicate values in location and time columns")
    logger.warning(f"Dropping")
    df.drop_duplicates(subset=['location', 'utc_datetime', 'lst_datetime'], inplace=True, ignore_index=True)

  # Write to .csv
  df.to_csv(f"{DIR_NAME}/data/uscrn_updates.csv", index=False)

@task
def load_staging_table():
  """Upload uscrn_updates.csv file to BigQuery"""

  # Set schema and job_config
  schema = [
    bigquery.SchemaField("station_location", "STRING", mode="REQUIRED"), 
    bigquery.SchemaField("wbanno", "STRING", mode="REQUIRED"), 
    bigquery.SchemaField("crx_vn", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("utc_datetime", "DATETIME", mode="REQUIRED"), 
    bigquery.SchemaField("lst_datetime", "DATETIME", mode="REQUIRED"), 
    bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"), 
    bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"), 
    bigquery.SchemaField("t_calc", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("t_hr_avg", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("t_max", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("t_min", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("p_calc", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("solarad", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("solarad_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("solarad_max", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("solarad_max_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("solarad_min", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("solarad_min_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_type", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_max", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_max_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_min", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("sur_temp_min_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("rh_hr_avg", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("rh_hr_avg_flag", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("date_added", "DATETIME", mode="REQUIRED")
  ]

  jc = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    schema=schema,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE"   
  )
 
  # Set target table in BigQuery
  full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"
    
  # Read file to dataframe first -- direct loading creating schema issues
  df = pd.read_csv(f"{DIR_NAME}/data/uscrn_updates.csv", index=False)

  # Upload 
  job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=jc)
  job.result()
  
  # Log result 
  logger.info(f"Loaded {df.size} rows and {len(df.columns)} columns into {full_table_id}")

@task
def insert_table() -> None:
  utils.insert_table(f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}")
## ---------- DEFINING DAG ---------- ## 
@dag(
   schedule_interval=INTERVAL,
   start_date=START,
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def uscrn_dag():
    
    t1 = check_domain()
    t2 = check_last_added()
    t3 = get_new_file_urls(t2)

    # t3 = getUpdates(t2)
    # t4 = transform_df(t3)
    # t5 = uploadBQ()
    # t6 = appendLocal()

    t1 >> [t2,t3] # >> t4 >> [t5, t6]

dag = uscrn_dag()


