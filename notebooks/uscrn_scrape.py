import requests
import pandas as pd 
import numpy as np
import yaml 
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup


output_file = "/home/alex/deb-projects/team-week3/alex-work/airflow/data/uscrn.csv"

if os.path.isfile(output_file):
  raise Exception("Warning: uscrn.csv file already exists in ../airflow/data/")

columns = ['station_location','wbanno','utc_date','utc_time','lst_date','lst_time','crx_vn','longitude','latitude',
't_calc','t_hr_avg','t_max','t_min','p_calc','solarad','solarad_flag','solarad_max','solarad_max_flag','solarad_min',
'solarad_min_flag','sur_temp_type','sur_temp','sur_temp_flag','sur_temp_max','sur_temp_max_flag','sur_temp_min',
'sur_temp_min_flag','rh_hr_avg','rh_hr_avg_flag','soil_moisture_5','soil_moisture_10','soil_moisture_20',
'soil_moisture_50','soil_moisture_100','soil_temp_5','soil_temp_10','soil_temp_20','soil_temp_50','soil_temp_100']

base_url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/"

base_soup = BeautifulSoup(requests.get(base_url).content, "html.parser")

def getFileUrls(): 
  links = base_soup.find_all("a") # 'links' in this notebook will refer to <a> elements, not urls
  years = [str(x).zfill(1) for x in range(2000,2024)]
  year_links = [link for link in links if link['href'].rstrip('/') in years]

  file_urls = []
  for year_link in year_links: 
    year_url = base_url + year_link.get("href")
    response = requests.get(year_url) 
    soup = BeautifulSoup(response.content, 'html.parser')
    file_links = soup.find_all('a', href=re.compile(r'AK.*\.txt'))
    if file_links:
      new_file_urls = [year_url + link.getText() for link in file_links]
      file_urls.extend(new_file_urls)
  return file_urls

file_urls = getFileUrls()

def transform_dataframe(df):

  df = df.copy()
  # replace missing value designators
  df.replace([-99999,-9999], np.nan, inplace=True) # Can safely assume these are always missing values in every column they appear in
  df = df.filter(regex="^((?!soil).)*$") # vast majority of soil columns have missing data
  df.replace({'crx_vn':{-9:np.nan}}, inplace=True)

  # convert to datetimes
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # drop old date and time columns
  df.drop(['utc_date', 'utc_time', 'lst_date', 'lst_time'], axis=1, inplace=True)

  # reorder columns 
  cols = ['station_location','wbanno','crx_vn','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # add date-added column
  df['date_added_utc'] = datetime.utcnow() 

  return df 

def process_rows(file_urls, row_limit, output_file):
    
    # Get rows for current batch
    rows = []
    regex = r"([St.]*[A-Z][a-z]+_*[A-Za-z]*).*.txt" 
    current_idx=0
    for i, url in enumerate(file_urls[current_idx:]):
      # Get location from url
      file_name = re.search(regex, url).group(0)
      station_location = re.sub("(_formerly_Barrow.*|_[0-9].*)", "", file_name)
      # Get results, add station location
      response = requests.get(url)
      soup = BeautifulSoup(response.content, 'html.parser')
      soup_lines = [station_location + " " + line for line in str(soup).strip().split("\n")]
      new_rows = [re.split('\s+', row) for row in soup_lines]
      # Add to list
      rows.extend(new_rows)
      if len(rows) >= row_limit:
        current_idx=i
        break

    # Create dataframe for current batch
    df = pd.DataFrame(rows, columns=columns)

    # Transform dataframe
    df = transform_dataframe(df)

    # Write dataframe to CSV
    if os.path.isfile(output_file):
        df.to_csv(output_file, mode='a', header=False, index=False)
    else:
      with open(output_file, "w") as fp:
        df.to_csv(fp, index=False)
    if len(rows) >= row_limit:
        # Recursively process remaining rows
        remaining_urls = file_urls[current_idx:]
        process_rows(remaining_urls, row_limit, output_file)
    else: 
        rows.clear()

process_rows(file_urls=file_urls, row_limit=100000, output_file=output_file)