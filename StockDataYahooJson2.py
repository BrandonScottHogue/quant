# -*- coding: utf-8 -*-
"""
Created on Sun Nov 19 11:13:45 2017

@author: Cameron
"""

import pandas as pd
from sqlalchemy import create_engine
import requests
import numpy as np
import datetime as dt
import math
#from io import StringIO
import sqlalchemy as sqla
#import time
db_host = 'localhost'
db_user = 'serviceuser'
db_pass = 'longJNUG'
db_name = 'quantdb'
uri = ("mysql+mysqldb://" + db_user + ":" + db_pass + 
                       "@" + db_host + "/" + db_name)
print(uri)
query = 'SELECT ticker FROM symbol'
engine = create_engine(uri)
#tickerIDs = pd.read_sql('SELECT id FROM symbol',con=engine)
tickers = pd.read_sql('SELECT ticker FROM symbol',con=engine)
path = 'https://query2.finance.yahoo.com/v6/finance/quote?symbols='
data = pd.DataFrame()
start = math.ceil(len(tickers)/3)
step = start
stop = 3*math.ceil(len(tickers)/3)+1
sRng = 0
#for r in range(5,11,5):
for r in range(start,stop,step):
#  print(sRng)
#  print(r)
  ticStr = tickers[sRng:r].to_csv(line_terminator=',',index=False,header=False)[:-1]
  sRng = r
#  print(path + ticStr)
  dfStr = requests.get(path + ticStr).text[27:-15]
  df = pd.read_json(dfStr,orient='records')
  data = pd.concat([data,df],ignore_index=True)

#idx = 0
#for sym in data['symbol']:
#  data['symbol_id'][idx] = tickers.loc[tickers['ticker'] == sym].index
#  idx += 1
#data['symbol_id'][data['symbol'] == tickers['ticker']]=tickers.index[data['symbol'] == tickers['ticker']]   
now = dt.datetime.utcnow()  
data.insert(0,'date', now)
data.insert(0,'symbol_id', 0)
tickers = tickers[tickers['ticker'].isin(data['symbol'])]
data['symbol_id'] = tickers.index
cols = ['symbol_id','date','ask','askSize','bid','bidSize','bookValue',
        'openInterest','postMarketPrice','priceToBook',
        'regularMarketPrice','regularMarketVolume']
if set(cols) < set(list(data)):
    cols = ['symbol_id','date','ask','askSize','bid','bidSize','bookValue',
            'openInterest','postMarketPrice','priceToBook',
            'regularMarketPrice','regularMarketVolume']
    data = data[cols]
    data = data.rename(columns={'askSize':'ask_size','bidSize':'bid_size',
                                'bookValue':'book_value',
                                'openInterest':'open_interest',
                                'postMarketPrice':'post_market_price',
                                'priceToBook':'price_to_book',
                                'regularMarketPrice':'price',
                                'regularMarketVolume':'current_day_volume'})
else:
    cols = ['symbol_id','date','ask','askSize','bid','bidSize','bookValue',
            'openInterest','priceToBook','regularMarketPrice',
            'regularMarketVolume']
    data = data[cols]
    data = data.rename(columns={'askSize':'ask_size','bidSize':'bid_size',
                                'bookValue':'book_value',
                                'openInterest':'open_interest',
                                'priceToBook':'price_to_book',
                                'regularMarketPrice':'price',
                                'regularMarketVolume':'current_day_volume'})

data.to_sql(con = engine, name='intraday_data', if_exists='append',index=False)
#            dtype = {'ask':sqla.DECIMAL(19,4),'ask_size':sqla.BIGINT,
#                     'bid':sqla.DECIMAL(19,4),'bid_size':sqla.BIGINT, 
#                     'book_value':sqla.DECIMAL(19,6),
                     #'eps_forwards':sqla.DECIMAL(19,6),
                     #'eps_trailing_year':sqla.DECIMAL(19,6),
                     #'forward_pe':sqla.DECIMAL(19,11),
                     #'open_interest':sqla.BIGINT,
#                     'post_market_price':sqla.DECIMAL(19,6),
#                     'price_to_book':sqla.DECIMAL(19,10),
                     #'quote_type':sqla.VARCHAR(8),
#                     'price':sqla.DECIMAL(19,6),
#                     'current_day_volume':sqla.BIGINT,
#                     'shares_outstanding':sqla.BIGINT})
                     #'trailing_dividend_rate':sqla.DECIMAL(19,6),
                     #'trailing_pe':sqla.DECIMAL(19,10)})
