#!/usr/bin/env python
# Tools to be used by the iexplotter.py script

###################################
###################################

import iex_tools
import os
import sys
import string
import datetime
import operator
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymongo
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
import pandas
from pandas.plotting import register_matplotlib_converters

################################################
################################################

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
days = mdates.DayLocator()  # every day
yearFmt = mdates.DateFormatter('%Y')
monthFmt = mdates.DateFormatter('%m')
dayFmt = mdates.DateFormatter('%d')
yearMonthFmt = mdates.DateFormatter('%Y-%m')

################################################
################################################

register_matplotlib_converters()

def plot_portfolio_return( portfolios, startDate ):

    spy_charts = iex_tools.mdb_get_chart(["SPY"]).sort_values(by="date", ascending=True, axis="index")
    spy_charts.reset_index(drop=True, inplace=True)
    fst_index = spy_charts[spy_charts.date >= startDate].index[0]-1
    spy_charts = spy_charts[spy_charts.index >= fst_index]
    spy_dates = spy_charts["date"].tolist()
    spy_close = spy_charts["close"].tolist()
    spy_return = [100.*((i/spy_close[0])-1.0) for i in spy_close]
    spy_datetime = [datetime.datetime.strptime(i, "%Y-%m-%d") for i in spy_dates]
    #print( spy_dates )
    #Loop through portfolios
    for portfolio in portfolios:
        #if portfolio not in ["stocks50mcap50B"]:
        #    continue
        #Get portfolio performance data
        perf_table = iex_tools.mdb_get_performance([portfolio], startDate).sort_values(by="date", ascending=True, axis="index")
        #print( perf_table )
        perf_dates = perf_table["date"].tolist()
        perf_dates.insert(0, spy_dates[0])
        perf_percent = perf_table["percentReturn"].tolist()
        #perf_close.insert(0, 0.0)
        #print( perf_dates )
        #print( perf_close )
        #print( len(perf_dates) )
        #print( len(perf_close) )
        #for i in range( len(perf_dates) ):
        #    print( '%s %d' % (perf_dates[i], perf_close[i]) )
        perf_return = [0.0]
        for percent in perf_percent:
            perf_return.append( ((((100.0+percent)/100.0)*((100.0+perf_return[-1])/100.0))-1.0)*100.0 )
        #perf_return = [100.*((i/perf_close[0])-1.0) for i in perf_close]
        #print( perf_return )
        #print( len( perf_return ) )
        #print( len( perf_dates ) )

        #Convert dates to datetime format
        perf_datetime = [datetime.datetime.strptime(i, "%Y-%m-%d") for i in perf_dates]

        # Plot the graph
        fig, ax = plt.subplots()
        #lines = ax.plot( spy_dates, spy_return, perf_dates, perf_return )
        l1 = ax.plot( spy_datetime, spy_return, label="S&P 500" )
        l2 = ax.plot( perf_datetime, perf_return, label="Portfolio" )

        # Format lines
        #l1, l2 = lines
        plt.setp(l1, color='orange')
        plt.setp(l2, color='purple')

        #Create legend
        plt.legend(loc="upper left")

        # Format the ticks
        ax.xaxis.set_major_locator(months)
        ax.xaxis.set_major_formatter(yearMonthFmt)
        ax.xaxis.set_minor_locator(days)

        # Set axis limits
        date_min = spy_dates[0]
        date_max = spy_dates[-1]
        ax.set_xlim( date_min, date_max )

        # Format the coords message box
        def price(x):
                return '$%1.2f' % x
        ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        ax.format_ydata = price
        ax.grid(True)

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        fig.autofmt_xdate()

        plt.xlabel( "Date" )
        plt.ylabel( "% Return" )
        plt.title( portfolio )
        #plt.show()
        plt.savefig(portfolio+".png")

def print_portfolio_holdings( portfolios, date ):

    #Get symbol data
    symbols = iex_tools.mdb_get_symbols()[["symbol","name"]]
    company = iex_tools.mdb_get_company(symbols["symbol"].tolist())
    #print( symbols )
    for portfolio in portfolios:
        #if portfolio not in ["stocks50mcap50B"]:
        #    continue
        #Get holding info on date
        holdings = iex_tools.mdb_get_holdings(portfolio, date)
        #print( holdings )
        #Join symbol info to get stock name
        holdings = pandas.merge(holdings, symbols, how='inner', left_on=["symbol"], right_on=["symbol"], sort=False)
        holdings = pandas.merge(holdings, company, how='inner', left_on=["symbol"], right_on=["symbol"], sort=False)
        print( "###########################################################" )
        print( "Holdings for portfolio " + portfolio + "..." )
        print( "###########################################################" )
        #print( holdings )
        for index, holding in holdings.iterrows():
            print( '%-10s %s' % ( holding['symbol'], holding['name'] ) )
            print( "Industry: " + holding['industry'] )
            print( "Description: " + holding['description'] )
            print( "Sector: " + holding['sector'] )
            print()
        print() 

