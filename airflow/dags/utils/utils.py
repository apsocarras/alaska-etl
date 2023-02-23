import pandas as pd 
  
## ----- NWS_DAG UTILS ----- ## 

def nwsURL(row):
  """Construct NWS forecast url from latitude and longitude columns"""
  url = f"https://forecast.weather.gov/MapClick.php?lat={row['latitude']}&lon={row['longitude']}&unit=0&lg=english&FcstType=digital&menu=1"
  return url

def ffList(ls:list) -> list:
  """Like ffill() from pandas, except for lists"""
  for i in range(len(ls)):
    if not ls[i] and i > 0:
        ls[i] = ls[i-1]
  return ls

def getColsFromTable(table:list, location:str):
  """Get cols from list of <tr> elements"""
  cols = [[ele.getText() for ele in tr.find_all("font")] for tr in table] # these are rows in the table's current landscape orientation
  location_col = ['location']
  location_col.extend([location]*24)
  cols.insert(1, location_col)
  cols.insert(19, location_col) # for second table
  return cols

def getDict(col_list:list):
  """Get dictionary from list of columns (which are also lists)"""
  data_map = {}
  for col in col_list:
    if col[0] not in data_map.keys(): # cols from first half of table
      data_map[col[0]] = col[1:]
    else: # cols from second half
      data_map[col[0]].extend(col[1:])
  data_map['Date'] = ffList(data_map['Date'])
  return data_map

  ## ----- USCRN_DAG UTILS ----- ## 


# def nanCheck(df:pd.DataFrame, msg:str, logger) -> None:  (TO-DO)
#   """Checks f any rows contain NaNs in a dataframe; logs any such rows and raises exception"""

#   nan_rows = df[df.isna().any(axis=1)]
#   if not nan_rows.empty: 
#     logger.