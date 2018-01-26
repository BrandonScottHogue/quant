import pandas as pd
from sqlalchemy import create_engine
import requests
from io import StringIO
import sqlalchemy as sqla
import time

class ReturnError(Exception):
  pass
class ReturnError1(ReturnError):
  pass
class ReturnError2(ReturnError):
  pass
#def errorCheck(df):
#  if len(df) < 3:
#    df = requests.get(path + tic + endPath).text
#    df = pd.read_csv(StringIO(df))
#hist = pd.read_csv('C:/Users/Cameron/Desktop/Stocks/Data/HistoricalData.csv',index_col='ticker')
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
path = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='
endPath = '&interval=1min&apikey=ACM6VY3MGAIDFUDS&datatype=csv'
#path = "http://finance.yahoo.com/d/quotes.csv?s="
#endPath = "&f=ohgvabj2dqr1ee9e7e8j4b4p5p6r5s7s6"
#sRng = 0
idx = 0
tableLength = 0
failedSyms = []
for tic in tickers['ticker']:
  print(tic)
  try:
    
    df = requests.get(path + tic + endPath).text
    time.sleep(1)
    df = pd.read_csv(StringIO(df))
    if len(df) < 3:
      raise ReturnError1
  except ReturnError1:
    print("Return Error: Trying again")
    try:
      time.sleep(.5)
      df = requests.get(path + tic + endPath).text
      df = pd.read_csv(StringIO(df))
      if len(df) < 3:
        raise ReturnError2
    except ReturnError2:
      print("2nd Return Error: Skipping symbol " + tic)
      failedSyms.append(tic)
      idx += 1
      continue
      
#  errorCheck(df)
  df.insert(0,'symbol_id',idx)
  idx += 1
  df.insert(0,'id',range(tableLength,tableLength + len(df)))
  tableLength += len(df)
  
  if idx == 1:
    df.to_sql(con = engine, name='minute_data', if_exists='replace',
              index=False,dtype = {'id':sqla.BIGINT,'symbol_id':sqla.BIGINT,
                        'timestamp':sqla.DateTime,'open':sqla.DECIMAL(19,4),
                        'high':sqla.DECIMAL(19,4),'low':sqla.DECIMAL(19,4),
                        'close':sqla.DECIMAL(19,4),'volume':sqla.BIGINT})
  else:
    df.to_sql(con = engine, name='minute_data', if_exists='append', index=False)
  time.sleep(.5)
with engine.connect() as con:
  con.execute('ALTER TABLE `minute_data` ADD PRIMARY KEY (`id`);')
failedSymbols = pd.DataFrame()
failedSymbols['symbols'] = failedSyms
failedSymbols.to_sql(con=engine, name='failed_symbols',if_exists='replace')
#  df['symbol_id'] = tickers.index[tickers.ticker == tic].tolist()
#  ticStr = tickers[sRng:r].to_csv(line_terminator='+',index=False,header=False)[:-1]
#  sRng = r
#  df = requests.get(path + ticStr + endPath).text
#  print(path + ticStr + endPath+'\n')
#tickers = tickers.to_csv(line_terminator='+',index=False,header=False)
#print(tickers)