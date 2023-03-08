import pandas as pd 
import numpy as np
import os 
import re
import csv
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

## --- Google Cloud --- ## 

# GCP/BigQuery information
with open(f"{DIR_NAME}/config/gcp-config.yaml", "r") as fp:
  gcp_config = full_load(fp)
PROJECT_ID = gcp_config['project-id']
DATASET_ID = gcp_config['dataset-id']
MAIN_TABLE_ID = "uscrn_wind"

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
def check_domain() -> None:
    """Checks connection to USCRN main domain"""
    utils.check_connection(domain_url="https://ncei.noaa.gov", logger=logger)

@task
def get_update_cutoff() -> int: # Integer representation of datetime
  """
  Args:
    None 
  
  Returns: 
    last_update (int): Integer repr. of earliest hour get_updates() should retrieve updates for 
  """
  
  query = f"""
  SELECT utc_datetime 
  FROM {DATASET_ID}.{MAIN_TABLE_ID}
  ORDER BY utc_datetime DESC LIMIT 1
  """
  query_job = bq_client.query(query)
  result = query_job.result()

  row = next(result)
  latest_datetime = row['utc_datetime'] + dt.timedelta(hours=1)
  latest_date = dt.datetime.strftime(latest_datetime, format="%Y%m%d")
  latest_hour= dt.datetime.strftime(latest_datetime, format="%H%M")

  update_cutoff = int(latest_date + latest_hour)

  return update_cutoff

@task 
def get_wind_file_urls() -> list:
  """Get URLs to subhourly files containing wind data
  
  Args 
    None:
  Returns 
    file_urls (list): List of urls containing subhourly wind_data files for 2023 
  """

  url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/subhourly01/2023/" 

  soup = utils.get_soup(url)

  file_urls = [url + link.getText() for link in soup.find_all("a", href=re.compile(r'AK.*\.txt'))]

  logger.info(f"get_wind_file_urls returned {file_urls}")

  return file_urls

@task
def get_updates(update_cutoff:int, file_urls:list) -> None:
  """Scrape new wind data and write to .csv 
  
  Args: 
    file_urls (list): List of urls to .txt files
    last_utc_datetime(int): Latest utc_datetime value from wind table

  Returns:
    wind_updates (list): List of new rows containing wind data 
  """

  wind_updates = []
  bad_rows = []
  for url in file_urls:
    # Get location from url
    station_location = utils.get_station_location(url)
    # Get new rows 
    soup = utils.get_soup(url, delay=1)
    lines = [re.split('\s+', line) for line in str(soup).strip().splitlines()]
    # Iterate backwards from end of list, stopping when date is prior to last_bq_update
    for i in range(len(lines)-1, -1, -1):
      # end loop when old data is reached 
      if int(lines[i][1] + lines[i][2]) <= update_cutoff:
        break 
      # log rows with erroneous wind data
      elif float(lines[i][-2]) < 0 or lines[i][-1] == "3": 
        bad_rows.append([station_location] + lines[i]) 
      else:
        wind_updates.append([station_location] + lines[i][:5] + [lines[i][-2]])

  logger.debug(f"{len(bad_rows)}/{len(bad_rows) + len(wind_updates)} rows had bad wind data: {bad_rows}")

  return wind_updates

@task
def transform_updates(wind_updates:list) -> None:
  """Read wind_updates from get_updates() to dataframe, transform, and return as dictionary""" 

  ## Read the data  
  colnames = ['station_location','wbanno','utc_date','utc_time',
  'lst_date','lst_time',"wind_1_5"]

  df = pd.DataFrame(wind_updates, columns=colnames)

  ## Transform data
  df['wind_1_5'] = df['wind_1_5'].astype(float)

  # convert to datetimes
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # round to nearest hour 
  df['utc_datetime'] = df['utc_datetime'].dt.floor("H")
  df['lst_datetime'] = df['lst_datetime'].dt.floor("H")

  # aggregate by hour
  df = df.groupby(['station_location','wbanno','utc_datetime','lst_datetime'])['wind_1_5'].mean().round(3).reset_index()
  df.rename({"wind_1_5":"wind_hr_avg"}, axis=1, inplace=True)

  # sort by date
  df.sort_values(["station_location", "utc_datetime"], inplace=True)

  ## Change datetime columns back to strings before passing to XCOM
  df['utc_datetime'] = df['utc_datetime'].astype(str)
  df['lst_datetime'] = df['lst_datetime'].astype(str)

  ## Write to .csv 
  df.to_csv(f"{DIR_NAME}/data/uscrn_wind_updates.csv", index=False)

@task
def load_staging_table() -> None: 
  """Read latest wind data updates csv and write to staging table in BigQuery"""


  # Set target table 
  table_id = f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}_staging"

  schema = [
    bigquery.SchemaField("station_location", "STRING", description="Location name for USCRN station", mode="REQUIRED"), 
    bigquery.SchemaField("wbanno", "STRING", description="The station WBAN number", mode="REQUIRED"), 
    bigquery.SchemaField("utc_datetime","DATETIME", description="UTC datetime of the observation", mode="REQUIRED"), 
    bigquery.SchemaField("lst_datetime","DATETIME", description="Local standard datetime of the observation (AKST)", mode="REQUIRED"),
    bigquery.SchemaField("wind_hr_avg","FLOAT", description="Average windspeed for the hour (m/s)", mode="NULLABLE")
  ]

  jc = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    schema=schema,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE", 
    destination_table_description="Hourly wind speed (m/s) measured at USCRN stations, aggregated from 5 minute measurements"
  )

  ## Upload from dataframe 
  # df = pd.DataFrame(transformed_dict)
  # job = bq_client.load_table_from_dataframe(df, table_id, job_config=jc)
  # job.result()

  # Upload from file 
  with open(f"{DIR_NAME}/data/uscrn_wind_updates.csv", "rb") as f: 
   job = bq_client.load_table_from_file(f, table_id, job_config=jc)
  job.result()

  # Log results 
  table = bq_client.get_table(table_id)
  logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {table_id}")

@task
def insert_to_main_table() -> None: 
  """Add data to main dataset from staging table"""
  utils.insert_table(f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}", logger, bq_client)

@dag(
   schedule_interval=INTERVAL,
   start_date=START,
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
   max_active_runs=1
)
def uscrn_wind_dag():
  
  t1 = check_domain()
  t2 = get_update_cutoff()
  t3 = get_wind_file_urls()
  t4 = get_updates(t2,t3)
  t5 = transform_updates(t4)
  t6 = load_staging_table()
  t7 = insert_to_main_table()
 
  t1 >> [t2,t3] >> t4  >> t5 >> t6  >> t7

dag = uscrn_wind_dag()
