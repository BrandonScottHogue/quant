#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 12 23:44:42 2017

@author: brandon
"""



import MySQLdb
import pandas as pd
import TACalculations as TACalc

mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                user='serviceuser', passwd='longJNUG', 
                db='quantdb')

sqlstring = "SELECT DISTINCT ticker FROM symbol INNER JOIN stock_data ON symbol.id = stock_data.symbol_id;"
df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
mysql_cn.close()    

#This calculates the SMA for all data in stock_data - it's intensive
#Looping, row by row through symbols(technically the dataframe row of the ticker column)
for row in df_mysql.itertuples():
    print(getattr(row,"ticker"))
    SMA_DF = TACalc.calcSMA((getattr(row,"ticker")))
    print(SMA_DF)