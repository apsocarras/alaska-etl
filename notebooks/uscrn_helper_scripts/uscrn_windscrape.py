
import requests
import pandas as pd 
import numpy as np
import os
import re
import csv
from datetime import datetime
from bs4 import BeautifulSoup
from uscrn_utils.utils import get_soup, get_file_urls, get_station_location



def get_raw_rows(file_urls, row_limit, output_file) -> None:
  """
   file.

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
    soup = get_soup(url, delay=.5)
    lines = [re.split('\s+', line) for line in str(soup).strip().splitlines()]
    # We're only scraping this data for the wind information, so we ignore rows that don't have any (i.e wind < 0)
    wind_cols = [[station_location] + line[:5] + line[-2:] for line in lines if float(line[-2]) >= 0]
    # Add to list
    rows.extend(wind_cols)
    if len(rows) >= row_limit:
      current_idx=i
      break

  # Write rows to CSV
  mode = "a" if os.path.isfile(output_file) else "w"
  with open(output_file, f"{mode}") as f:
    writer = csv.writer(f)
    writer.writerows(rows)

  # Recursively process remaining rows     
  if len(rows) >= row_limit:
      remaining_urls = file_urls[current_idx:]
      del rows
      get_raw_rows(remaining_urls, row_limit, output_file)
  else: 
      return 

  
  















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
    soup = get_soup(url, delay=.5)
    lines = [re.split('\s+', line) for line in str(soup).strip().splitlines()]
    # We're only scraping this data for the wind information, so we ignore rows that don't have any (i.e wind < 0)
    wind_cols = [[station_location] + line[:5] + line[-2:] for line in lines if float(line[-2]) >= 0]
    # Add to list
    rows.extend(wind_cols)
    if len(rows) >= row_limit:
      current_idx=i
      break

  # Define column names
  columns = ['station_location','wbanno','utc_date','utc_time',
  'lst_date','lst_time',"wind_1_5", "wind_flag"]
  
  # Create dataframe for current batch
  df = pd.DataFrame(rows, columns=columns)

  ####  Transform dataframe  #### 

  # convert wind_1_5 to float -- drop any negative measurements
  df['wind_1_5'] = df['wind_1_5'].astype(float)

  # convert to datetimes
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # round to nearest hour 
  df['utc_datetime'] = df['utc_datetime'].dt.floor("H")
  df['lst_datetime'] = df['lst_datetime'].dt.floor("H")

  # calculate hourly averages 
  df = df.groupby(['station_location','wbanno','utc_datetime','lst_datetime','wind_flag'])['wind_1_5'].mean().reset_index()

  # rename wind column 
  df.rename({"wind_1_5":"wind_hr_avg"}, axis=1, inplace=True)

  #### --------------------- #####

  # Write dataframe to CSV
  if os.path.isfile(output_file):
      df.to_csv(output_file, mode='a', header=False, index=False)
      del df
      df = pd.DataFrame()
      rows.clear()
  else:
    with open(output_file, "w") as fp:
      df.to_csv(fp, index=False)
      del df
      df = pd.DataFrame
      rows.clear()
  # Recursively process remaining rows     
  if len(rows) >= row_limit:
      remaining_urls = file_urls[current_idx:]
      process_rows(remaining_urls, row_limit, output_file)
  else: 
      return 


if __name__ == "__main__":

  file_urls = get_file_urls("subhourly01") # directory containing wind data

  output_file = "../../data/uscrn_wind.csv"

  if os.path.isfile(output_file):
    raise Exception(f"{output_file} already exists")

  process_rows(file_urls=file_urls, row_limit=300000, output_file=output_file)