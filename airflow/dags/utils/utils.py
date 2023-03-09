import requests
import pandas as pd 
import re
import time
import itertools
from logging import Logger 
import datetime as dt 
from bs4 import BeautifulSoup
from google.cloud import bigquery 
from google.api_core.exceptions import NotFound

##-------------------------------------------- ##
# Shared utilities (USCRN and NWS)
def get_soup(url:str, delay=0) -> BeautifulSoup:
  """Simple wrapper for getting beautiful soup object from url with sleep delay
  
  Args: 

  url (str): url you're scraping

  delay (int): seconds you want to wait between next request (default 0)
  """
  result = requests.get(url)
  time.sleep(delay)
  return BeautifulSoup(result.content, "html.parser") 

def check_connection(domain_url:str, logger:Logger) -> None: 
  """Check connection to domain"""

  if domain_url not in ("https://ncei.noaa.gov", "https://weather.gov"):
    raise Exception(f"Invavlid domain given {domain_url}")
  
  try:
    response = requests.get(domain_url, timeout=5)
    if response.status_code == 200:
        logger.info(f"Connection to {domain_url} is successful")
    else:
          logger.info(f"Connection to {domain_url} failed with status code {response.status_code}")
  except requests.exceptions.Timeout as e:
      logger.info(f"Connection to {domain_url} timed out: {e}")
  except requests.exceptions.RequestException as e:
      logger.info(f"Connection to {domain_url} failed: {e}")

def insert_table(full_table_id:str, logger:Logger, bq_client:bigquery.Client) -> None:
  """Insert staging table into main data table -- create main table if needed"""

  insert_query=f"""
    INSERT INTO {full_table_id} 
    SELECT *, CURRENT_TIMESTAMP() as date_added_utc
    FROM {full_table_id}_staging 
    """

  try: 
    query_job = bq_client.query(insert_query) 
    query_job.result()
  except NotFound:
    logger.info(f"Table {full_table_id} does not exist. Creating.")

    create_query = f"""
      CREATE TABLE {full_table_id}
      AS
      SELECT *, CURRENT_TIMESTAMP() as date_added_utc
      FROM {full_table_id}_staging
    """
    query_job = bq_client.query(create_query)
    query_job.result()
    
##-------------------------------------------- ##
# USCRN Specific Utilities

def _get_year_urls(uscrn_directory:str) -> list: 
  """
  Retrieves the URLs for every year's page in the given USCRN directory.
  
  Arguments:
  uscrn_directory (str): Either 'hourly02' or 'subhourly01' (i.e. source of wind data)

  Returns:
  year_urls (list): A list of URLs for every year's page.
  """

  if uscrn_directory not in ("hourly02", 'subhourly01'):
    raise Exception(f"Invalid directory given: {uscrn_directory} -- give 'hourly02' or 'subhourly01'")
  
  url = f"https://www.ncei.noaa.gov/pub/data/uscrn/products/{uscrn_directory}/"
  soup = get_soup(url, 1)

  # Wind data is first available in 2012
  start_year = 2012 if uscrn_directory == "subhourly01" else 2000

  links = soup.find_all("a") 
  years = [str(y).zfill(1) for y in range(start_year, 2024)]
  year_urls = [url + link['href'] for link in links if link['href'].rstrip('/') in years]
  return year_urls


def get_file_urls(uscrn_directory:str) -> list: 
  """
  Retrieves the URLs for every file contained on each year's page in the given USCRN directory

  Arguments:
  uscrn_directory (str): Either 'hourly02' or 'subhourly01'

  Returns: 
  file_urls (list): A list of file URLs.
  """

  if uscrn_directory not in ("hourly02", 'subhourly01'):
    raise Exception(f"Invalid directory given: {uscrn_directory} -- give 'hourly02' or 'subhourly01'")

  year_urls = _get_year_urls(uscrn_directory)

  file_urls = []
  for url in year_urls: 
    soup = get_soup(url)
    file_links = soup.find_all('a', href=re.compile(r'AK.*\.txt'))
    if file_links:
      new_file_urls = [url + link.getText() for link in file_links]
      file_urls.extend(new_file_urls)
  return file_urls

def get_station_location(url) -> str: 
  """
  Extracts the name of the station from a given file URL.
  
  Args:
  url (str): The URL to extract the station name from.
  
  Returns:
  station_location (str): The name of the station.
  """
  regex = r"([St.]*[A-Z][a-z]+_*[A-Za-z]*).*.txt" 
  file_name = re.search(regex, url).group(0)
  station_location = re.sub("(_formerly_Barrow.*|_[0-9].*)", "", file_name)
  return  station_location

##-------------------------------------------- ##
# NWS Specific Utilities

def flatten(ls:list): 
  """Flattens/unnests a list of lists by one layer"""
  return list(itertools.chain.from_iterable(ls)) 

def _ff_list(ls:list) -> list:
  """Forward fill the values in a list"""
  for i in range(len(ls)):
    if not ls[i] and i > 0:
        ls[i] = ls[i-1]
  return ls

def get_nws_url(row:pd.Series) -> str:
  """
  Get url for the next 48 hours of forecasts from latitude and longitude columns
  
  Args: 
  row (pd.Series): The current row of the dataframe

  Returns: 
  url (str): The url for the next 48 hours of forecasts
  """
  lat, lon = row["latitude"], row["longitude"]
  url = f"https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w12=fog&AheadHour=0&Submit=Submit&FcstType=digital&textField1={lat}&textField2={lon}&site=all&unit=0&dd=&bw=&menu=1"
  return url

def get_last_update_nws(soup:BeautifulSoup) -> str:
  """
  Find the "Last Updated" value from a BeautifulSoup object, transform to a datetime in AKST

  Args:
  soup (BeautifulSoup): A Beautiful Soup representation of a particular NWS forecast page

  Returns: 
  last_update_dt (str): String representation of time page was last updated (AKST) (Format: "%I:%M%p %b %d %Y")
  """
  last_update_tag = soup.find('td', string=lambda text: text and 'Last Update:' in text)
  last_update_text = re.sub("Last Update: |\s(?=pm|am)|AKST |,", "", last_update_tag.getText())
  # last_update_dt = dt.datetime.strptime(last_update_text, "%I:%M%p %b %d %Y") -- needs to be string to push to XCOMs
  return last_update_text

def extract_table_data(soup:BeautifulSoup, location:str) -> list:
  """
  Extracts 48hr forecast table data from a Beautiful Soup object as a list of lists

  Args: 
  table_records (list): List of <tr> elements containing NWS forecast data

  location (str): The name of the place the forecast is for; used for filling out added "location" column 

  Returns:
  table (list): List of lists containing table data 
  """
  table_records = soup.find_all("table")[5].find_all("tr")

  colspan = table_records[0] # 48hr data is divided into two tables by two colspan elements
  table = [tr for  tr in table_records if tr != colspan] # vertically concat tables by removing colspan elements

  table = [[ele.getText() for ele in tr.find_all("font")] for tr in table] 

  # Add location column 
  location_col = ['location']
  location_col.extend([location]*24) # fill out to match length of other columns
  table.insert(1, location_col)  # for first half of table
  table.insert(19, location_col) # for second half of table

  # Add last_update_nws column 
  last_update_nws = ["last_update_nws"]
  last_update_nws.extend([get_last_update_nws(soup)] * 24)
  table.insert(1, last_update_nws)
  table.insert(19, last_update_nws) 

  return table

def transpose_as_dict(table:list) -> dict:
  """
  Takes the list of lists generated by extract_table_data() and transposes it (flip orientation) by casting as a dictionary
  
  Args:
  table (list): list of lists of columnar data generated by extract_table_data()

  Returns: 
  data_map (dict): Dictionary representation of table, transposed and ready to be made into a dataframe
  """
  data_map = {}
  for col in table: # Table is still "landscape-oriented"
    if col[0] not in data_map.keys(): # cols from first half of table
      data_map[col[0]] = col[1:]
    else: # cols from second half
      data_map[col[0]].extend(col[1:])

  data_map['Date'] = _ff_list(data_map['Date'])

  return data_map