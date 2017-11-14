#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 20:15:29 2017

@author: brandon
"""
import pandas as pd
import MySQLdb
import plotly.offline
from plotly.graph_objs import *

#make the connection string
mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                user='serviceuser', passwd='longJNUG', 
                db='quantdb')

#build query and read into dataframe
df_mysql = pd.read_sql('SELECT date, ticker, open, close, high,low FROM stock_data_ticker WHERE ticker = "AMD" AND stock_data_ticker.date > "2016-12-31";', con=mysql_cn)    

#here set the max rows to show as 10000, default is 1000 I think
pd.set_option('display.max_rows', 100)

#show the dataframe
#print(df_mysql)
trace0 = Candlestick(x=df_mysql.date,
                       open=df_mysql.open,
                       high=df_mysql.high,
                       low=df_mysql.low,
                       close=df_mysql.close)

data = [trace0]

layout = Layout(
        title = 'AMD Stock Price',
        yaxis = {'title': 'Stock Price'},
        showlegend = True,
        height = 768,
        width = 1024,
)
fig = dict( data = data, layout = layout)
plotly.offline.plot(fig, filename = "plot.html")
#close the connection
mysql_cn.close()
