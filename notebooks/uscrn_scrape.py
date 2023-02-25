import requests
import pandas as pd 
import numpy as np
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup


def get_year_urls() -> list: 
  """
  Retrieves the URLs for every year's page in the USCRN index.
  
  Returns:
  year_urls (list): A list of URLs for every year's page.
  """

  url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/"
  response = requests.get(url)
  soup = BeautifulSoup(response.content, "html.parser")


  links = soup.find_all("a") 
  years = [str(x).zfill(1) for x in range(2000,2024)]
  year_urls = [url + link['href'] for link in links if link['href'].rstrip('/') in years]
  return year_urls

def get_file_urls() -> list: 
  """
  Retrieves the URLs for every file contained on each year's page.

  Returns: 
  file_urls (list): A list of file URLs.
  """
  year_urls = get_year_urls()

  file_urls = []
  for url in year_urls: 
    response = requests.get(url) 
    soup = BeautifulSoup(response.content, 'html.parser')
    file_links = soup.find_all('a', href=re.compile(r'AK.*\.txt'))
    if file_links:
      new_file_urls = [url + link.getText() for link in file_links]
      file_urls.extend(new_file_urls)
  return file_urls

def transform_dataframe(df) -> pd.DataFrame:
  """
  Transforms a Pandas DataFrame created from a list of lists in process_rows().
    
  Args:
  df (pandas.DataFrame): The DataFrame to be transformed.
  
  Returns:
  transformed_df (pandas.DataFrame): The transformed DataFrame.
  """
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

def get_station_location(url) -> str: 
  """
  Extracts the name of the station from a given URL.
  
  Args:
  url (str): The URL to extract the station name from.
  
  Returns:
  station_location (str): The name of the station.
  """
  regex = r"([St.]*[A-Z][a-z]+_*[A-Za-z]*).*.txt" 
  file_name = re.search(regex, url).group(0)
  station_location = re.sub("(_formerly_Barrow.*|_[0-9].*)", "", file_name)
  return  station_location

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

  # Define column names for dataframes
  columns = ['station_location','wbanno','utc_date','utc_time','lst_date','lst_time','crx_vn','longitude','latitude',
  't_calc','t_hr_avg','t_max','t_min','p_calc','solarad','solarad_flag','solarad_max','solarad_max_flag','solarad_min',
  'solarad_min_flag','sur_temp_type','sur_temp','sur_temp_flag','sur_temp_max','sur_temp_max_flag','sur_temp_min',
  'sur_temp_min_flag','rh_hr_avg','rh_hr_avg_flag','soil_moisture_5','soil_moisture_10','soil_moisture_20',
  'soil_moisture_50','soil_moisture_100','soil_temp_5','soil_temp_10','soil_temp_20','soil_temp_50','soil_temp_100']

  # Get rows for current batch
  rows = []
  current_idx=0
  for i, url in enumerate(file_urls[current_idx:]):
    # Get location from url
    station_location = get_station_location(url)
    # Get new rows 
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
    
    # Recursively process remaining rows     
    if len(rows) >= row_limit:
        remaining_urls = file_urls[current_idx:]
        process_rows(remaining_urls, row_limit, output_file)
    else: 
        return 

if __name__ == "__main__":

  output_file = "../data/uscrn.csv"

  if os.path.isfile(output_file):
    raise Exception(f"{output_file} already exists")

  process_rows(file_urls=get_file_urls(), row_limit=100000, output_file=output_file)