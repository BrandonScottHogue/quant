#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 20:15:29 2017

@author: brandon
"""
import pandas as pd
import MySQLdb

#make the connection string
def fillDataframeFromSQL(ticker,startdate,enddate):
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    #build query and read into dataframe
    sqlstring = "SELECT date,ticker,open,close,high,low FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\" AND date BETWEEN \"%s\" AND \"%s\";" % (ticker,startdate,enddate)
    print(sqlstring)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    
    #here set the max rows to show as 10000, default is 10 I think
    pd.set_option('display.max_rows', 10)
    
    #show the dataframe
    print(df_mysql)
    
    #close the connection
    mysql_cn.close()

fillDataframeFromSQL('TSLA','2017-1-1','2017-6-1')