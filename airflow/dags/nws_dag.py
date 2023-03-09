import pandas as pd
import numpy as np
import requests
import re
import datetime as dt 
import os
from io import BytesIO
from yaml import full_load
from bs4 import BeautifulSoup
# Utilities imports: 
import utils.utils as utils
# Airflow imports: 
from airflow.decorators import dag, task
# GCP imports: 
from google.cloud import bigquery, storage
from google.oauth2 import service_account 
from google.api_core.exceptions import NotFound

## ---------- GLOBAL VARIABLES ---------- ## 

## Path information
PATH = os.path.abspath(__file__)
DIR_NAME = os.path.dirname(PATH)

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
STAGING_TABLE_ID = 'nws_staging'
MAIN_TABLE_ID = 'nws' 

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


## ---------- DEFINING TASKS ---------- ## 

@task 
def get_forecast_dict() -> dict:
  """Get dictionary of forecast data for next 6 days from various points in Alaska"""

  nws_urls = locations_df.apply(utils.get_nws_url, axis=1)
  url_map = dict(zip(locations_df['station_location'], nws_urls))

  combined_table = []
  for location, url in url_map.items():
    soup_list = [utils.get_soup(url + f"&AheadHour={hr}") for hr in (0,48,96)]
    table_list = utils.flatten([utils.extract_table_data(soup, location) for soup in soup_list])
    combined_table.extend(table_list)

  forecast_dict = utils.transpose_as_dict(combined_table)
  
  return forecast_dict

@task
def transform_forecast(forecast_dict) -> dict: 
  """Cast dictionary from get_forecast_dict() to a dataframe, transform, and cast back to dict"""
  ## Create dataframe
  df = pd.DataFrame(forecast_dict)
  
  ## Edit column headers 
  df.columns = [col.lower() for col in df.columns] 
  df.rename(columns=lambda x: re.sub('°|\(|\)', '', x), inplace=True)
  df.rename(columns=lambda x: re.sub('%', 'pct', x), inplace=True)
  df.rename(columns=lambda x: re.sub(' ', '_', x.strip()), inplace=True)
  
  ## Replace missing values
  # Replace missing values in gust with zero -- gust is never *actually* 0 so no masking
  # Replace missing values in windchill with an explicity np.NaN
  df.replace({'gust':{'':0}, 'wind_chill_f':{'':np.nan}}, inplace=True)

  ## Datetime Transformations
  cur_year = dt.datetime.now().year
  dt_strings = df['date'] + '/' + str(cur_year) + ' ' + df['hour_akst'] + ':00 AKST'
  # Local time (AKST)
  df['lst_datetime'] = pd.to_datetime(dt_strings, format='%m/%d/%Y %H:%M AKST')
  # UTC time
  akst_offset = dt.timedelta(hours=9)
  df['utc_datetime'] = df['lst_datetime'] + akst_offset

  ## Drop duplicates in composite key columns 
  duplicates = df.duplicated(subset=["location", "lst_datetime"], keep=False)
  duplicate_rows = df[duplicates]
  if not duplicate_rows.empty:
    print(f"Warning: {len(duplicate_rows)} rows have duplicate values in location and lst_datetime")
    print(f"Dropping")
    df.drop_duplicates(subset=['location', 'lst_datetime'], inplace=True, ignore_index=True)

  ## Reorder columns 
  col_names = ['location', 'utc_datetime', 'lst_datetime'] + list(df.columns[4:-2]) + ["last_update_nws"]
  df = df[col_names]

  ## Change datetime columns back to strings before passing to XCOM
  df['utc_datetime'] = df['utc_datetime'].astype(str)
  df['lst_datetime'] = df['lst_datetime'].astype(str)

  transformed_dict = df.to_dict()
  return transformed_dict


@task
def load_staging_table(transformed_dict:dict) -> None: 
  """Upload dataframe from get_forecast_df() to BigQuery staging table"""
  
  # Create dataframe
  df = pd.DataFrame(transformed_dict)

  # Re-read datetime columns to datetime
  df['utc_datetime'] = pd.to_datetime(df['utc_datetime'])
  df['lst_datetime'] = pd.to_datetime(df['lst_datetime'])
  df['last_update_nws'] = pd.to_datetime(df['last_update_nws'])

  # Set Schema
  schema = [
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"), 
    bigquery.SchemaField("utc_datetime", "DATETIME", mode="REQUIRED"), 
    bigquery.SchemaField("lst_datetime", "DATETIME", mode="REQUIRED"), 
    bigquery.SchemaField("temperature_f", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("dewpoint_f", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("wind_chill_f", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("surface_wind_mph", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("wind_dir", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("gust", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("sky_cover_pct", "INTEGER", mode="NULLABLE"), 
    bigquery.SchemaField("precipitation_potential_pct", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("relative_humidity_pct", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rain", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("thunder", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("snow", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("freezing_rain", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sleet", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("fog", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("last_update_nws", "DATETIME", mode="NULLABLE")
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

  # Upload to BigQuery
  ## If any required columns are missing values, include name of column in error message
  try: 
    job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=jc)
    job.result()
  except Exception as e:
    error_message = str(e)
    if 'Required column value for column index' in error_message:
      start_index = error_message.index('Required column value for column index') + len('Required column value for column index: ')
      end_index = error_message.index(' is missing', start_index)
      missing_column_index = int(error_message[start_index:end_index])
      missing_column_name = list(df.columns)[missing_column_index]
      error_message = error_message[:start_index] + f'{missing_column_name} ({missing_column_index})' + error_message[end_index:]
    raise Exception(error_message) 
  
  job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=jc)
  job.result()

  # Log result 
  table = bq_client.get_table(full_table_id)
  print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {full_table_id}\n")

@task
def insert_table() -> None: 
  """Insert staging table into the main data table -- creates the table if it doesn't exist yet"""
  
  insert_query=f"""
    INSERT INTO {DATASET_ID}.{MAIN_TABLE_ID} 
    SELECT *, CURRENT_TIMESTAMP() as date_added_utc
    FROM {DATASET_ID}.{STAGING_TABLE_ID}
    """

  try: 
    query_job = bq_client.query(insert_query) 
    query_job.result()
  except NotFound:
    print(f"Table {DATASET_ID}.{MAIN_TABLE_ID} does not exist. Creating.")
    create_query = f"""
      CREATE TABLE {DATASET_ID}.{MAIN_TABLE_ID}
      AS
      SELECT *, CURRENT_TIMESTAMP() as date_added_utc
      FROM {DATASET_ID}.{STAGING_TABLE_ID}
    """
    query_job = bq_client.query(create_query)
    query_job.result()
    
  full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}"
  table = bq_client.get_table(full_table_id)
  print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {full_table_id}\n")


@dag(
   schedule_interval=INTERVAL,
   start_date=START, 
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def nws_dag():
    t1 = get_forecast_dict()
    t2 = transform_forecast(t1)
    t3 = load_staging_table(t2)
    t4 = insert_table()

    t1 >> t2 >> t3 >> t4

dag = nws_dag()
