import requests
import pandas as pd 
import numpy as np
import os
import re
import time
import itertools
import datetime as dt 
from bs4 import BeautifulSoup



## Shared utilities (USCRN and NWS)
def get_soup(url:str, delay=0) -> BeautifulSoup:
  """Simple wrapper for getting beautiful soup object from url with sleep delay
  
  Args: 

  url (str): url you're scraping

  delay (int): time you want to wait between next request (default 0)
  """
  result = requests.get(url)
  time.sleep(delay)
  return BeautifulSoup(result.content, "html.parser") 

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

## NWS specific utilities 

def flatten(ls:list): 
  """Flattens/unnests a list of lists by one layer"""
  return list(itertools.chain.from_iterable(ls)) 

def ff_list(ls:list) -> list:
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

def get_last_update_nws(soup:BeautifulSoup) -> dt.datetime:
  """
  Find the "Last Updated" value from a NWS forecast page, transform to a datetime 
  Args:
  soup (BeautifulSoup): A Beautiful Soup representation of a particular NWS forecast page

  Returns: 
  last_update_dt (datetime): Datetime representation of time page was last updated (AKST)
  """
  last_update_tag = soup.find('td', string=lambda text: text and 'Last Update:' in text)
  last_update_text = re.sub("Last Update: |\s(?=pm|am)|AKST |,", "", last_update_tag.getText())
  last_update_dt = dt.datetime.strptime(last_update_text, "%I:%M%p %b %d %Y")

  # (TO-DO) Apply AKST timezone
  
  return last_update_dt