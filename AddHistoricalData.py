# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 13:30:00 2017

@author: Cameron
"""
import datetime
import time
import requests
from lxml import html
import MySQLdb as mdb
import numpy as np
from math import ceil
import Utils as ut

def updateSql(data):
    data = tuple(map(tuple,data))

    db_host = 'localhost'
    db_user = 'serviceuser'
    db_pass = 'longJNUG'
    db_name = 'quantdb'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    cur = con.cursor()
    try:
#        cur.execute("SET SESSION sql_mode = ''")
        for i in range(0, int(ceil(len(data) / 100.0))):
            
          cur.executemany(finalStr, data[i*100:(i+1)*100-1])
    except mdb.DatabaseError:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        con.close()

db_host = 'localhost'
db_user = 'serviceuser'
db_pass = 'longJNUG'
db_name = 'quantdb'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

tickerIDs = ut.getColumn("symbol", "id", con=con)
tickers = ut.getColumn("symbol", "ticker", con=con)
#exchanges = ut.getColumn("symbol", "exchange", con=con)

# tickers = pd.read_sql("SELECT ticker FROM symbol", con=con).values
# exchanges = pd.read_sql("SELECT exchange FROM symbol", con=con).values

# =============================================================================
#session = requests.Session()
#session.get('https://finance.yahoo.com')
#r = requests.get("https://finance.yahoo.com")
#print(r.cookies)
#cook = r.cookies['B']
cook = '1k0o2lhcn7j6q&b=3&s=9b'
cookie = dict(B=cook)
print(cookie)
page = requests.get("https://query1.finance.yahoo.com/v1/test/getcrumb", cookies=cookie)

tree = html.fromstring(page.content)
print(html.tostring(tree).decode('unicode_escape'))
#tree = etree.HTML(page.content)
crumb = tree.xpath('//p')
print(crumb[0].text)
#/html/body/pre
#session.cookies.get_dict()
#https://query1.finance.yahoo.com/v1/test/getcrumb"


baseUrl = "https://query1.finance.yahoo.com/v7/finance/download/"

sD = datetime.date(1970, 1, 1)
t = datetime.time(00,00,00)
eD = datetime.datetime.now().date()
startDate = int(time.mktime(datetime.datetime.combine(sD,t).timetuple()))
endDate = int(time.mktime(datetime.datetime.combine(eD,t).timetuple()))

idx = 0
for tic in tickers:
    tic = tic[1:-1]
    # print(str(tic)[2:-2])
    print(tic)
    url = (baseUrl + tic + '?period1=' + str(startDate) + '&period2=' +
            str(endDate) + '&interval=1d&events=history&crumb=' + crumb[0].text)
    csvFile = requests.get(url, cookies=cookie).text.split('\n')
    if len(csvFile) < 3:
        print("Skipped")
        idx += 1
        continue
    tickerCol = np.array([str(tickerIDs[idx]) for i in range(len(csvFile)-2)]).transpose()
#    exchangeCol = np.array([exchanges[idx] for i in range(len(csvFile)-2)]).transpose()
    idx += 1
#    for i in csvFile[1:2]:
#        rowVals = i.split(',')
#        for j in range(len(rowVals)):
#            rowVals[j] = rowVals[j].strip()
#    data = [str(r.split(',')).strip().split(',') for r in csvFile[1:2]]
#    data = [str(s).strip() for s in data]
    data = [r.split(',') for r in csvFile[1:-1]]
    for i in range(len(data)):
        for j in range(len(data[0])):
            data[i][j] = data[i][j].strip()
            if data[i][j] == "null":
                data[i][j] = None
    data = np.array(data)
    numCols = len(data[0,:])
    fData = np.zeros((len(data),numCols+1),dtype=object)
    fData[:,1:] = data
#    fData[:,0] = exchangeCol
    fData[:,0] = tickerCol
    columnStr = ("symbol_id, date, open, high, low, close,"
                 " adjusted_close, volume")
    insertStr = ("%s, " * (numCols+1))[:-2]
    finalStr = "INSERT INTO stock_data (%s) VALUES (%s)" % (columnStr, insertStr)
    updateSql(fData)
#    data = csv.reader(csvFile.split('\n')[1:-1], delimiter=',')
#    data = np.array(data)
#    data = tuple(map(tuple,data))

#print(csvFile.content.decode('unicode_escape'))
# =============================================================================
