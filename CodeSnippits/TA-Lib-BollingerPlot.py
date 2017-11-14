#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 12 23:01:01 2017

@author: brandon
"""
import pandas as pd
import talib as ta
from talib import MA_Type
import MySQLdb
import plotly.offline as po
import plotly.graph_objs as go

#make the connection string
def plotBollinger(ticker):
    mysql_cn= MySQLdb.connect(host='localhost', port=3306,
                    user='serviceuser', passwd='longJNUG', 
                    db='quantdb')
    
    #build query and read into dataframe
    sqlstring = "SELECT date,ticker,close FROM stock_data_ticker WHERE stock_data_ticker.ticker = \"%s\";" % (ticker)
    print(sqlstring)
    df_mysql = pd.read_sql(sqlstring,con=mysql_cn)
    mysql_cn.close()
    
    #show the dataframe
    #print(df_mysql)
    
    df_mysql['upper'], df_mysql['middle'], df_mysql['lower'] = ta.BBANDS(df_mysql.close.values.astype('float64'), matype=MA_Type.T3)

    close = go.Scatter(
            x = df_mysql.date,
            y = df_mysql.close,
            mode = 'lines',
            name = 'close',
            line = dict(width = .5)
        )
        
    upper = go.Scatter(
        x = df_mysql.date,
        y = df_mysql.upper,
        mode = 'lines',
        name = 'upper',
        line = dict(width = 1)
    )

    middle = go.Scatter(
        x = df_mysql.date,
        y = df_mysql.middle,
        mode = 'lines',
        name = 'middle',
        line = dict(width = 1)
    )

    lower = go.Scatter(
        x = df_mysql.date,
        y = df_mysql.lower,
        mode = 'lines',
        name = 'lower',
        line = dict(width = 1)
    )
    
    data = [close, upper, middle, lower]

    layout = go.Layout(
            title = '%s Bollinger Bands' % (ticker),
            #yaxis = {'title': 'Stock Price'},
            showlegend = True,
            height = 900,
            width = 1800,
            xaxis=dict(
                showgrid=True,
                zeroline=False,
                showline=True,
                mirror='ticks',
                gridcolor='#bdbdbd',
                gridwidth=1,
                zerolinecolor='#969696',
                zerolinewidth=4,
                linecolor='#636363',
                linewidth=1
                ),
            yaxis=dict(
                autotick=False,
                ticks='outside',
                tick0=0,
                dtick=10,
                ticklen=5,
                tickwidth=1,
                tickcolor='#bdbdbd',
                title = 'Stock Price',
                showgrid=True,
                zeroline=False,
                showline=True,
                mirror='ticks',
                gridcolor='#bdbdbd',
                gridwidth=1,
                zerolinecolor='#969696',
                zerolinewidth=4,
                linecolor='#636363',
                linewidth=1
                )
    )
    fig = dict( data = data, layout = layout)
    po.offline.plot(fig, filename = "BollingerBand-plot-%s.html" % ticker)
    
    #print(df_mysql)
    #close the connection

plotBollinger('AAL')
