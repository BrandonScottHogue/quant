#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 20:15:29 2017

@author: brandon
"""
import pandas as pd
import talib as ta
from talib import MA_Type
import MySQLdb

def calcBollinger(ticker):#Returns Date, Closed, and 3 Bollinger Bands
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['upper'], df_mysql['middle'], df_mysql['lower'] = ta.BBANDS(df_mysql.close.values.astype('float64'), matype=MA_Type.T3)
    return df_mysql

#Calling the calculateBollinger function will return a dataframe with date, closed, upper, middle, and lower

def calcSMA(ticker):
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['SMA']= ta.SMA(df_mysql.close.values.astype('float64'))
    return df_mysql

def calcMomentum(ticker, time_period):#Example time period = 6
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['Momentum']= ta.MOM(df_mysql.close.values.astype('float64'),timeperiod = time_period)
    return df_mysql

def calcMACD(ticker,fast_period,slow_period,signal_period):#Ex: Fast=12,Slow=26,Sig=9
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['MACD'],df_mysql['MACD Signal'],df_mysql['MACD History']= ta.MACD(df_mysql.close.values.astype('float64'),fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period)
    return df_mysql

def calcCCI(ticker, time_period):#Example time period = 14
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,high,low,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['CCI']= ta.CCI(df_mysql.high.values.astype('float64'),df_mysql.low.values.astype('float64'),df_mysql.close.values.astype('float64'),timeperiod = time_period)
    return df_mysql

def calcRSI(ticker,time_period):#Example time period = 14
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    sqlstring = "SELECT date,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    
    df_mysql['RSI'] = ta.RSI(df_mysql.close.values.astype('float64'),timeperiod=time_period)
    return df_mysql

#returnedDF = calcBollinger('AAL')

#returnedDF = calcSMA('AAL')

#returnedDF = calcMomentum('AAL',6)

#returnedDF = calcMACD('AAL',12,26,9)

#returnedDF = calcCCI('AAPL',14)
    
returnedDF = calcRSI('AAPL',14)

print(returnedDF)

