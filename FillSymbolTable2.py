# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 23:11:20 2017

@author: Cameron
"""
import os
import csv
import numpy as np
import datetime
import MySQLdb as mdb
from math import ceil

def makeData(index, etf):
    indexDir = os.getcwd() + "/Indexes/" + index + ".csv"
    print(indexDir)
    data = convCsv(indexDir)
    # Create numpy array from Nasdaq.csv
    #data = np.genfromtxt(indexDir,delimiter=",",dtype=str)
    stock = ["stock" for i in range(len(data))]
    data[:,8] = stock
    data[0,8] = "instrument"

    # Create numpy array from ETFList.csv
    indexDir = os.getcwd() + "/Indexes/" + etf + ".csv"
    etfData = convCsv(indexDir)

    # If symbol is in both arrays change instrument column from
    # 'stock' to 'ETF'
    idx = range(1,len(data))
    for i in idx:
        if data[i,0] in etfData[:,0]:
            data[i,8] = "ETF"
    return data

def convCsv(indexDir):
    with open(indexDir, newline='',encoding='utf-8') as f:
        reader = csv.reader(f)
        data = list(reader)
        return np.array(data)

def formatData(index,data):
    now = datetime.datetime.utcnow()
    length = len(data)-1

    indexCol = np.array([index for i in range(length)])
    indexCol.shape = (length,1)

    tickers = data[1:,0]
    tickers.shape = (length,1)

    names = data[1:,1]
    names.shape = (length,1)

    instrument = data[1:,8]
    instrument.shape = (length,1)

    sectors = data[1:,5]
    sectors.shape = (length,1)

    industry = data[1:,6]
    industry.shape = (length,1)

    ipoYear = data[1:,4]
    ipoYear.shape = (length,1)

    createdDate = [[str(now)] for i in range(length)]
    lastUpDate = [[str(now)] for i in range(length)]

    dbData = np.concatenate((indexCol,tickers,names,instrument,sectors,industry,ipoYear,createdDate,lastUpDate),axis=1)

    print(dbData)
    return dbData

def ETFData(index):
    indexDir = os.getcwd() + "/Indexes/" + index + ".csv"
    print(indexDir)
    data = convCsv(indexDir)
    
    
    
    now = datetime.datetime.utcnow()
    length = len(data)-1
    noneCol = np.array([None for i in range(length)])
    data = data[1:,:]
#    data[:,]
    print(noneCol)
#    dbData = data[:,]

def updateSql(data):
    data = tuple(map(tuple,data))

    db_host = 'localhost'
    db_user = 'serviceuser'
    db_pass = 'longJNUG'
    db_name = 'quantdb'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    cur = con.cursor()
    try:
        for i in range(0, int(ceil(len(data) / 100.0))):
          cur.executemany(finalStr, data[i*100:(i+1)*100-1])
    except mdb.DatabaseError:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        cur.close()

indexes = ["NASDAQ","NYSE","AMEX","ETFList"]
data = ETFData(indexes[3])
data1 = makeData(indexes[0],indexes[3])
data1 = formatData(indexes[0],data1)
data2 = makeData(indexes[1],indexes[3])
data2 = formatData(indexes[1],data2)
data3 = makeData(indexes[2],indexes[3])
data3 = formatData(indexes[2],data3)
data = [data1,data2,data3]

columnStr = ("exchange, ticker, name, instrument, sector, industry, "
                "ipo_year, created_date, last_update_date")
insertStr = ("%s, " * 9)[:-2]
finalStr = "INSERT INTO symbol (%s) VALUES (%s)" % (columnStr, insertStr)

#for i in range(3):
#    updateSql(data[i])