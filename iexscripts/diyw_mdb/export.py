#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: A short script that uses the 'iex_tools' module to 
#        extract stock information from the IEX API
# Usage: python3 iex_main.py

import os
import sys
import pandas
import pymongo
#from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
import datetime
import iexscripts.diyw_mdb.query as mdb_query

#Export latest stock lists
def export_stock_list():
    print( "Exporting stock list to json" )
    currDate = datetime.datetime.now().strftime("%Y-%m-%d")
    #currDate = "2019-05-01"
    latestStockList = mdb_query.get_stock_list(currDate, "latest")
    #latestStockList = mdb_algo.calculate_top_stocks(currDate)
    #Check that list is ordered correctly!
    #Export stock list dataframe to json file
    latestStockList.to_json(path_or_buf="output/json/stock_list.json", orient="records")

#Export latest performance
def export_performance():
    print( "Exporting performance tables to json" )
    #Start date
    startDate = "2018-07-02"
    #Get list of portfolios
    portfolios = mdb_query.get_portfolios(startDate)["portfolioID"].tolist()
    #Export SPY performance
    spy_charts = mdb_query.get_chart(["SPY"]).sort_values(by="date", ascending=True, axis="index")
    spy_charts.reset_index(drop=True, inplace=True)
    spy_quotes = mdb_query.get_quotes(["SPY"]).sort_values(by="date", ascending=True, axis="index")
    spy_quotes.reset_index(drop=True, inplace=True)
    spy_quotes = spy_quotes[spy_quotes.date > spy_charts.date.iloc[-1]]
    spy_charts = spy_charts.append( spy_quotes, ignore_index=True, sort=False )
    fst_index = spy_charts[spy_charts.date >= startDate].index[0]-1
    spy_charts = spy_charts[spy_charts.index >= fst_index]
    spy_dates = spy_charts["date"].tolist()
    spy_close = spy_charts["close"].tolist()
    spy_return_vals = [100.*((i/spy_close[0])-1.0) for i in spy_close]
    performance = pandas.DataFrame()
    for index, date in enumerate(spy_dates):
        doc = { "date": date, "SPY": spy_return_vals[index] }
        performance = performance.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    #spy_charts["return"] = 100.*(spy_charts["close"]/spy_charts["close"].iloc[0])-1.0)
    #spy_charts = spy_charts[["date","return"]]
    #spy_return.to_json(path_or_buf="output/json/spy_performance.json", orient="records")
    #Get list of portfolios
    portfolios = mdb_query.get_portfolios(startDate)["portfolioID"].tolist()
    #Loop through portfolios
    for portfolio in portfolios:
        #Get portfolio performance data
        perf_table = mdb_query.get_performance([portfolio], startDate).sort_values(by="date", ascending=True, axis="index")
        #print( perf_table )
        perf_dates = perf_table["date"].tolist()
        perf_dates.insert(0, spy_charts["date"].iloc[0])
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
        pf_return = pandas.DataFrame()
        for index, date in enumerate(perf_dates):
            doc = { "date": date, portfolio: perf_return[index] }
            pf_return = pf_return.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
        performance = pandas.merge(performance,pf_return,how='inner',left_on=['date'],right_on=['date'],sort=False)
    #print( performance )
    performance.to_json(path_or_buf="output/json/performance.json", orient="records")
    for portfolio in portfolios:
        perf = performance.loc[:, performance.columns.isin(["date",portfolio])]
        perf = perf.rename(columns={portfolio: 'return'})
        perf.to_json(path_or_buf="output/json/"+portfolio+"_performance.json", orient="records")
    perf = performance.loc[:, performance.columns.isin(["date","SPY"])]
    perf = perf.rename(columns={'SPY': 'return'})
    perf.to_json(path_or_buf="output/json/spy_performance.json", orient="records")
