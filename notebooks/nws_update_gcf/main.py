import pandas as pd
import numpy as np
import re
import datetime as dt 
import logging 
from io import BytesIO
# GCP imports: 
from google.cloud import bigquery, storage, logging as cloud_logging 
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
# Utils
import utils.utils as utils 
## ^^ For the actual package it will just be "utils.utils"

PROJECT_ID = "alaska-scrape"
DATASET_ID = "weather"
STAGING_TABLE_ID = "nws_staging"
MAIN_TABLE_ID = "nws"

SCHEMA =  [
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

## ---------- LOGGING ---------- ## 
# Cloud logging client
logger_client = cloud_logging.Client()

# Cloud logging handler
handler = logger_client.get_default_handler()

# Create logger with cloud handler
logger = logging.getLogger(__name__)
logger.addHandler(handler)

# Set logging levels 
logger.setLevel(logging.INFO)
handler.setLevel(logging.INFO)

# Format logger 
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Confirm logger is working  
logger.info(f"Running daily scrape of NWS Weather Forecasts in Alaska")

## ---------- CLOUD STORAGE ---------- ## 
storage_client = storage.Client()
bucket = storage_client.bucket(f"{PROJECT_ID}-bucket")

# Locations 
blob = bucket.blob("locations.csv")
content = blob.download_as_bytes()
locations_df = pd.read_csv(BytesIO(content))

## ---------- BIGQUERY ---------- ## 
bq_client = bigquery.Client()

def get_forecast_df() -> pd.DataFrame:
  """Get dataframe of forecast data for next 6 days from various points in Alaska"""

  nws_urls = locations_df.apply(utils.get_nws_url, axis=1)
  url_map = dict(zip(locations_df['station_location'], nws_urls))

  combined_table = []
  for location, url in url_map.items():
    soup_list = [utils.get_soup(url + f"&AheadHour={hr}") for hr in (0,48,96)]
    table_list = utils.flatten([utils.extract_table_data(soup, location) for soup in soup_list])
    combined_table.extend(table_list)

  forecast_dict = utils.transpose_as_dict(combined_table)
  forecast_df = utils.transform_df(forecast_dict)
  
  return forecast_df


def load_staging_table(df:pd.DataFrame) -> None:
  """Upload dataframe from transform_df() to BigQuery staging table"""

  jc = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    schema=SCHEMA,
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

  # Log result 
  table_ref = bq_client.get_table(full_table_id)
  logger.info(f"Loaded {table_ref.num_rows} rows and {table_ref.schema} columns")

  
def insert_table() -> None: 
  """Insert staging table into the main data table -- creates the table if it doesn't exist yet"""
  
  insert_query=f"""
    INSERT INTO {DATASET_ID}.{MAIN_TABLE_ID} 
    SELECT *, CURRENT_TIMESTAMP() as date_added
    FROM {DATASET_ID}.{STAGING_TABLE_ID}
    """
  
  full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}"

  try: 
    query_job = bq_client.query(insert_query) 
    query_job.result()
  except NotFound:
    logger.info(f"Table {DATASET_ID}.{MAIN_TABLE_ID} does not exist. Creating.")

    # Adding date_added to SCHEMA 
    schema = SCHEMA + [bigquery.SchemaField("date_added", "TIMESTAMP", mode="REQUIRED")]

    table = bigquery.Table(full_table_id, schema=schema)
    table = bq_client.create_table(table)

    query_job = bq_client.query(insert_query)
    query_job.result()
    
  table = bq_client.get_table(full_table_id)
  logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {full_table_id}\n")



df = get_forecast_df()

load_staging_table(df)

insert_table()