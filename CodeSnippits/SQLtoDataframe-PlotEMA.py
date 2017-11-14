#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 20:15:29 2017

@author: brandon
"""
import pandas as pd
import MySQLdb
#import plotly.plotly as py
import plotly.offline as po
import plotly.graph_objs as go

#make the connection string
def generateEMAPlot(ticker):
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
    df_mysql['7EMA'] = df_mysql['close'].ewm(span=7,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['14EMA'] = df_mysql['close'].ewm(span=14,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['21EMA'] = df_mysql['close'].ewm(span=21,min_periods=0,adjust=True,ignore_na=False).mean()
    df_mysql['45EMA'] = df_mysql['close'].ewm(span=45,min_periods=0,adjust=True,ignore_na=False).mean()
    
    #round
    df_mysql.close = df_mysql.close.round(2)
    df_mysql['7EMA'] = df_mysql['7EMA'].round(2)
    df_mysql['14EMA'] = df_mysql['14EMA'].round(2)
    df_mysql['21EMA'] = df_mysql['21EMA'].round(2)
    df_mysql['45EMA'] = df_mysql['45EMA'].round(2)
                    
    #here set the max rows to show as 10000, default is 10 I think
    pd.set_option('display.max_rows', 20)
    
    #show the dataframe
    #print(df_mysql)
    trace0 = go.Scatter(
        x = df_mysql.date,
        y = df_mysql.close,
        mode = 'lines',
        name = 'close'
    )
    
    trace1 = go.Scatter(
        x = df_mysql.date,
        y = df_mysql['7EMA'],
        mode = 'lines',
        name = '7 EMA',
        line = dict(width = 1)
    )

    trace2 = go.Scatter(
        x = df_mysql.date,
        y = df_mysql['14EMA'],
        mode = 'lines',
        name = '14 EMA',
        line = dict(width = 1)
    )

    trace3 = go.Scatter(
        x = df_mysql.date,
        y = df_mysql['21EMA'],
        mode = 'lines',
        name = '21 EMA',
        line = dict(width = 1)
    )
    
    trace4 = go.Scatter(
        x = df_mysql.date,
        y = df_mysql['45EMA'],
        mode = 'lines',
        name = '45 EMA',
        line = dict(width = 1)
    )

    data = [trace0, trace1, trace2, trace3, trace4]

    layout = go.Layout(
            title = '%s Stock Price' % (ticker),
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
    po.offline.plot(fig, filename = "EMA-plot-%s.html" % ticker)
    
    #close the connection
    mysql_cn.close()

generateEMAPlot('MU')