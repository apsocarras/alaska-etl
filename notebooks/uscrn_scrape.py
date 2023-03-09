import requests
import pandas as pd 
import numpy as np
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
from utils.utils import get_file_urls, get_station_location


def process_rows(file_urls, row_limit, output_file) -> None:
  """
  Processes a batch of rows from a list of URLs to extract weather station data and save it to a CSV file.
  Args:
    file_urls (list): A list of URLs where weather station data can be found.
    row_limit (int): The maximum number of rows to process per batch.
    output_file (str): The path to the output CSV file.
  Returns:
    None
  """
  # Get rows for current batch
  rows = []
  current_idx=0
  for i, url in enumerate(file_urls[current_idx:]):
    # Get location from url
    station_location = get_station_location(url)
    # Get new rows 
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    soup_lines = [station_location + " " + line for line in str(soup).strip().splitlines()]
    new_rows = [re.split('\s+', row) for row in soup_lines]
    # Add to list
    rows.extend(new_rows)
    if len(rows) >= row_limit:
      current_idx=i
      break

  # Define column names
  columns = ['station_location','wbanno','utc_date','utc_time','lst_date','lst_time','crx_vn','longitude','latitude',
  't_calc','t_hr_avg','t_max','t_min','p_calc','solarad','solarad_flag','solarad_max','solarad_max_flag','solarad_min',
  'solarad_min_flag','sur_temp_type','sur_temp','sur_temp_flag','sur_temp_max','sur_temp_max_flag','sur_temp_min',
  'sur_temp_min_flag','rh_hr_avg','rh_hr_avg_flag','soil_moisture_5','soil_moisture_10','soil_moisture_20',
  'soil_moisture_50','soil_moisture_100','soil_temp_5','soil_temp_10','soil_temp_20','soil_temp_50','soil_temp_100']
  
  # Create dataframe for current batch
  df = pd.DataFrame(rows, columns=columns)

  ####  Transform dataframe  #### 

  # Convert to fahrenheit 
  df = df.apply(pd.to_numeric, errors='ignore')
  df[['t_calc','t_hr_avg', 't_max', 't_min']].apply(lambda celsius: np.where(celsius > -90, celsius * 9/5 + 32, celsius))

  # Drop soil columns -- vast majority have missing data 
  df = df.filter(regex="^((?!soil).)*$")

  # convert to datetimes
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # drop old date and time columns
  df.drop(['utc_date', 'utc_time', 'lst_date', 'lst_time'], axis=1, inplace=True)

  # reorder columns 
  cols = ['station_location','wbanno','crx_vn','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # add date added column
  df['date_added_utc'] = datetime.utcnow()

  #### --------------------- #####

  # Write dataframe to CSV
  hdr = False if os.path.isfile(output_file) else True
  df.to_csv(output_file, mode="a+", header=hdr, index=False)
  
  # if os.path.isfile(output_file):
  #     df.to_csv(output_file, mode='a', header=False, index=False)
  # else:
  #   with open(output_file, "w") as fp:
  #     df.to_csv(fp, index=False)
  
  # Recursively process remaining rows     
  if len(rows) >= row_limit:
      remaining_urls = file_urls[current_idx:]
      process_rows(remaining_urls, row_limit, output_file)
  else: 
      return 

if __name__ == "__main__":

  file_urls = get_file_urls("hourly02") # 'hourly02' -- main data directory for USCRN

  output_file = "/home/alex/portfolio/projects/alaska-etl/airflow/dags/data/uscrn.csv"

  if os.path.isfile(output_file):
    raise Exception(f"{output_file} already exists")

  process_rows(file_urls=file_urls, row_limit=100000, output_file=output_file)