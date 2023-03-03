import requests
import pandas as pd 
import numpy as np
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup

def get_year_urls(directory:str) -> list: 
  """
  Retrieves the URLs for every year's page in the given USCRN directory.
  
  Arguments:
  directory (str): Either 'hourly02' or 'subhourly01'

  Returns:
  year_urls (list): A list of URLs for every year's page.
  """

  if directory not in ("hourly02", 'subhourly01'):
    raise Exception(f"Invalid directory given: {directory} -- give 'hourly02' or 'subhourly01'")
  
  url = f"https://www.ncei.noaa.gov/pub/data/uscrn/products/{directory}"
  response = requests.get(url)
  soup = BeautifulSoup(response.content, "html.parser")

  links = soup.find_all("a") 
  years = [str(x).zfill(1) for x in range(2000,2024)]
  year_urls = [url + link['href'] for link in links if link['href'].rstrip('/') in years]
  return year_urls

def get_file_urls(directory:str) -> list: 
  """
  Retrieves the URLs for every file contained on each year's page in the given USCRN directory

  Arguments:
  directory (str): Either 'hourly02' or 'subhourly01'

  Returns: 
  file_urls (list): A list of file URLs.
  """

  if directory not in ("hourly02", 'subhourly01'):
    raise Exception(f"Invalid directory given: {directory} -- give 'hourly02' or 'subhourly01'")

  year_urls = get_year_urls(directory)

  file_urls = []
  for url in year_urls: 
    response = requests.get(url) 
    soup = BeautifulSoup(response.content, 'html.parser')
    file_links = soup.find_all('a', href=re.compile(r'AK.*\.txt'))
    if file_links:
      new_file_urls = [url + link.getText() for link in file_links]
      file_urls.extend(new_file_urls)
  return file_urls

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
