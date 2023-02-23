from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import os
import re
import os 
from yaml import full_load
from bs4 import BeautifulSoup
# Utilities imports: 
from utils.utils import getColsFromTable, getDict, nwsURL
# Airflow imports: 
from airflow.decorators import dag, task
# GCP imports: 
from google.cloud import bigquery 
from google.oauth2 import service_account 

## ---------- GLOBAL VARIABLES ---------- ## 
# Path information
PATH = os.path.abspath(__file__)
DIR_NAME = os.path.dirname(PATH)
# GCP/BigQuery information
with open(f"{DIR_NAME}/data/bq-config.yaml", "r") as fp:
  bq_config = full_load(fp)
PROJECT_ID = bq_config['project-id']
DATASET_ID = 'alaska'
TABLE_ID = 'uscrn'
# Data Source URLs 
with open(f"{DIR_NAME}/data/sources.yaml", "r") as fp:
  SOURCES = full_load(fp)

## ---------- SET LOGGING ---------- ## 
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(f'/opt/airflow/logs/nws_dag_logs.txt')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


## ---------- DEFINING TASKS ---------- ## 
@task 
def getForecast():
  """Get dictionary of forecast data for next 48 hours from various points in Alaska"""
  locations = pd.read_csv(f"{DIR_NAME}/data/locations.csv")
  nws_urls = locations.apply(nwsURL, axis=1)
  loc_dict = dict(zip(locations['station_location'], nws_urls))

  col_list = []
  for location, url in loc_dict.items():
    result = requests.get(url)
    soup = BeautifulSoup(result.content, "html.parser")
    table48 = soup.find_all("table")[5].find_all("tr") # list of <tr> elements from main data table (really two tables combined: one for each day in next 48h period)
    colspan = table48[0]  # divided into two tables by two colspan elements
    table48 = [tr for  tr in table48 if tr != colspan] # remove colspan elements

    cols = getColsFromTable(table48,location)    
    col_list.extend(cols)
  
  return getDict(col_list)

@task
def transformDF(myDict): 
  """Cast dictionary from getForecast() to a dataframe, transform, and write (append) to .csv"""
  df = pd.DataFrame(myDict)
  df.columns = [col.lower() for col in df.columns] 
  df.replace({'':np.NaN, '--':np.NaN}, inplace=True)

  ## Datetime Transformations
  cur_year = datetime.now().year
  dt_strings = df['date'] + '/' + str(cur_year) + ' ' + df['hour (akst)'] + ':00 AKST'
  # Local time (AKST)
  df['lst_datetime'] = pd.to_datetime(dt_strings, format='%m/%d/%Y %H:%M AKST')
  # UTC time
  akst_offset = timedelta(hours=9)
  df['utc_datetime'] = df['lst_datetime'] + akst_offset

  # reorder columns 
  cols = ['location','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # timestamp column: track when forecast was accessed -- DAG will run every 48 hours
  df['date_added_utc'] = datetime.utcnow()

  # edit column headers 
  df.rename(columns=lambda x: re.sub('°|\(|\)', '', x), inplace=True)
  df.rename(columns=lambda x: re.sub('%', 'pct', x), inplace=True)
  df.rename(columns=lambda x: re.sub(' ', '_', x.strip()), inplace=True)

  # Logging DF  
  df_head = df.head(10)
  logger.info(f"Section of DataFrame:\n{df_head.to_string(index=False)}")

  # Write to csv
  hdr = False if os.path.isfile(f'{DIR_NAME}/data/nws_updates/forecasts.csv') else True
  with open(f"{DIR_NAME}/data/nws_updates/forecasts.csv", "a+") as fp:
    df.to_csv(fp, header=hdr, index=False)

@task
def load_data_to_bq() -> None:
    """Load the transformed data to BigQuery"""
    
    # Set credentials
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_file(
     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Create client
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # Set schema and job_config
    schema = [
      bigquery.SchemaField("location", "STRING", mode="REQUIRED"), 
      bigquery.SchemaField("utc_datetime", "DATETIME", mode="REQUIRED"), 
      bigquery.SchemaField("lst_datetime", "DATETIME", mode="REQUIRED"), 
      bigquery.SchemaField("temperature_f", "INTEGER", mode="REQUIRED"), 
      bigquery.SchemaField("dewpoint_f", "INTEGER", mode="REQUIRED"), 
      bigquery.SchemaField("wind_chill_f", "INTEGER", mode="REQUIRED"), 
      bigquery.SchemaField("surface_wind_mph", "INTEGER", mode="REQUIRED"), 
      bigquery.SchemaField("wind_dir", "STRING", mode="REQUIRED"), 
      bigquery.SchemaField("gust", "INTEGER", mode="NULLABLE"), 
      bigquery.SchemaField("sky_cover_pct", "INTEGER", mode="REQUIRED"), 
      bigquery.SchemaField("precipitation_potential_pct", "FLOAT", mode="REQUIRED"), 
      bigquery.SchemaField("relative_humidity_pct", "FLOAT", mode="REQUIRED"),
      bigquery.SchemaField("rain", "STRING", mode="NULLABLE"), 
      bigquery.SchemaField("thunder", "STRING", mode="NULLABLE"), 
      bigquery.SchemaField("snow", "STRING", mode="NULLABLE"), 
      bigquery.SchemaField("freezing_rain", "STRING", mode="NULLABLE"),
      bigquery.SchemaField("sleet", "STRING", mode="NULLABLE")
    ]

    df = pd.read_csv(f'{DIR_NAME}/data/nws_updates/forecasts.csv')
    
    jc = bigquery.LoadJobConfig(
      source_format = bigquery.SourceFormat.CSV,
      skip_leading_rows=1,
      autodetect=False,
      schema=schema,
      create_disposition="CREATE_IF_NEEDED",
      write_disposition="WRITE_APPEND"   
    )
 
    # Set target table in BigQuery
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Upload to BigQuery
    ## If any columns are missing values, include name of column in error message
    try: 
      job = client.load_table_from_dataframe(df, full_table_id, job_config=jc)
      job.result()
    except Exception as e:
      error_message = str(e)
      # modify error message to include the name of the missing column
      if 'Required column value for column index' in error_message:
        start_index = error_message.index('Required column value for column index') + len('Required column value for column index: ')
        end_index = error_message.index(' is missing', start_index)
        missing_column_index = int(error_message[start_index:end_index])
        # get the name of the missing column based on its index
        missing_column_name = list(df.columns)[missing_column_index]
        # modify the error message to include the name of the missing column
        error_message = error_message[:start_index] + f'{missing_column_name} ({missing_column_index})' + error_message[end_index:]
      raise Exception(error_message) 

    # Log result 
    table = client.get_table(full_table_id)
    print(f"Loaded {table.num_rows} rows and {table.schema} columns")

@dag(
   schedule_interval="@once",
   start_date=datetime.utcnow(),
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def nws_dag():
    t1 = getForecast()
    t2 = transformDF(t1)
    t3 = load_data_to_bq()

    t1 >> t2 >> t3

dag = nws_dag()
