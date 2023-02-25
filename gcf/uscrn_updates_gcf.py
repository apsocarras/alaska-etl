import requests
import pandas as pd 
import numpy as np
import os
import sys
import re
from yaml import full_load
from datetime import datetime
from bs4 import BeautifulSoup
from google.cloud import bigquery 
from google.cloud.exceptions import NotFound

## ---------- SET CLOUD LOGGING ---------- ## 
import logging
from google.cloud import logging as cloud_logging

# cloud logging client
client_log = cloud_logging.Client()

# cloud logging handler
handler = client_log.get_default_handler()
handler.setLevel(logging.INFO)

# create logger with cloud handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# format logger 
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# confirm logger is working  
logger.info(f"Running initial check (and scrape, if needed) of USCRN data")

## ---------- SET BIGQUERY CLIENT ---------- ## 

client = bigquery.Client() # running in cloud function, so no need to specify credentials

DATASET_ID = "alaska-scrape"
TABLE_ID = "uscrn"

try:
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)
    if table.num_rows != 0: 
      logger.info(f"{TABLE_ID} exists and is not empty.")
      sys.exit()
except NotFound:
    logger.info(f"Table {TABLE_ID} is not found.")
    logger.info(f"Creating {TABLE_ID} in dataset {DATASET_ID}")

columns = ['station_location','wbanno','utc_date','utc_time','lst_date','lst_time','crx_vn','longitude','latitude',
't_calc','t_hr_avg','t_max','t_min','p_calc','solarad','solarad_flag','solarad_max','solarad_max_flag','solarad_min',
'solarad_min_flag','sur_temp_type','sur_temp','sur_temp_flag','sur_temp_max','sur_temp_max_flag','sur_temp_min',
'sur_temp_min_flag','rh_hr_avg','rh_hr_avg_flag','soil_moisture_5','soil_moisture_10','soil_moisture_20',
'soil_moisture_50','soil_moisture_100','soil_temp_5','soil_temp_10','soil_temp_20','soil_temp_50','soil_temp_100']

updates_url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/"