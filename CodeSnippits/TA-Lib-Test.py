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

#make the connection string
def calculateBollinger(ticker):
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    #build query and read into dataframe
    sqlstring = "SELECT date,ticker,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    #print(sqlstring)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()    
    #show the dataframe
    #print(df_mysql)
    
    df_mysql['upper'], df_mysql['middle'], df_mysql['lower'] = ta.BBANDS(df_mysql.close.values.astype('float64'), matype=MA_Type.T3)
    return df_mysql

    #print(df_mysql)
    #close the connection
    mysql_cn.close()

returnedDF = calculateBollinger('AAL')
print(returnedDF)