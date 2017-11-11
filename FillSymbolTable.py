# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 21:14:59 2017

@author: Cameron
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

data = pd.DataFrame()
# Get exchange info from csv and store in dataframe data
indexes = ["NASDAQ","NYSE","AMEX"]
for idx in indexes:
  indexDir = os.getcwd() + "/Indexes/" + idx + ".csv"
  df = pd.read_csv(indexDir)
  # Fix info from csv
  df = df.replace('na',np.nan)
  df = df.drop("Unnamed: 8",axis=1)
  df = df.drop('LastSale',axis=1)
  df = df.drop('MarketCap',axis=1)
  df = df.drop('Summary Quote',axis=1)
  df.insert(0,'Exchange', idx)
  data = pd.concat([data,df],ignore_index=True)

# Add columns to data  
now = datetime.utcnow()  
data['Instrument'] = 'stock'
data['Date_created'] = now
data['Date_last_updated'] = now
colsData = data.columns.tolist()

# Get ETF info from csv cross check with info in data and add only what does
# not alread exist
indexDir = os.getcwd() + "/Indexes/ETFList.csv"
etf = pd.read_csv(indexDir)

colsEtf = etf.columns.tolist()
etf = etf.drop(colsEtf[2:6],axis=1)

etf['Instrument'] = 'ETF'
etf['Date_created'] = now
etf['Date_last_updated'] = now

# Changes Instrument to ETF if symbol appears in ETF info and drops rows in 
# etf so info is not duplicated 
data['Instrument'][data['Symbol'].isin(etf.index)] = 'ETF'
etf.drop(etf.index[~etf['Symbol'].isin(data.index)])
data = pd.concat([data,etf],ignore_index=True)
data = data[colsData]

# Create and fill symbol table 
# DELETES INFORMATION IN TABLE IF TABLE ALREADY EXISTS
db_host = 'localhost'
db_user = ''
db_pass = ''
db_name = ''
uri = ("mysql+mysqldb://" + db_user + ":" + db_pass + 
                       "@" + db_host + "/" + db_name)
print(uri)
engine = create_engine(uri)
data.to_sql(con = engine, name='symbol', if_exists='replace')
  