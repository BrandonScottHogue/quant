#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 20:15:29 2017

@author: brandon
"""
import pandas as pd
import MySQLdb

#make the connection string
def fillDataframeFromSQL(ticker):
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    #build query and read into dataframe
    sqlstring = "SELECT date,ticker,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    print(sqlstring)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    #df_mysql = df_mysql.pop('date')
    #df_mysql = df_mysql.set_index(pd.DatetimeIndex(df_mysql['date']))
    
    #compute exponential moving averages
    df_mysql['10EMA'] = df_mysql['close'].ewm(span=10,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['30EMA'] = df_mysql['close'].ewm(span=30,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['60EMA'] = df_mysql['close'].ewm(span=60,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['90EMA'] = df_mysql['close'].ewm(span=90,min_periods=0,adjust=True,ignore_na=False).mean()
    
    #round
    df_mysql.close = df_mysql.close.round(2)
    df_mysql['10EMA'] = df_mysql['10EMA'].round(2)
    df_mysql['30EMA'] = df_mysql['30EMA'].round(2)
    df_mysql['60EMA'] = df_mysql['60EMA'].round(2)
    df_mysql['90EMA'] = df_mysql['90EMA'].round(2)
                    
    #here set the max rows to show as 10000, default is 10 I think
    pd.set_option('display.max_rows', 20)
    
    #show the dataframe
    print(df_mysql)
    
    #close the connection
    mysql_cn.close()

fillDataframeFromSQL('TSLA')