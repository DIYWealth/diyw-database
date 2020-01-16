#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: Toolkit to access the IEX API and data stored in MongoDB.

import os
import sys
import json
import requests
import string
import datetime
import glob
import time
import pymongo
import bson
from pymongo import ASCENDING, DESCENDING
import numpy
import pandas
import iexscripts.diyw_mdb.query as mdb_query

def calculate_top_stocks_old(ref_date):
    """
    Calculate ranked list of stocks
    @params:
        ref_date    - Required  : date YYYY-MM-DD (Str)
    """

    #Get ranked stock list for given date
    symbols = mdb_query.get_active_companies().tolist()
    print( "Query earnings" )
    earnings = mdb_query.get_earnings(symbols, ref_date, "latest", "EPSReportDate")
    earnings = earnings[["EPSReportDate","actualEPS","fiscalEndDate","fiscalPeriod","symbol"]]
    #print( earnings )
    #Get financials within 6 months
    print( "Query financials" )
    sixMonthsBeforeDate = (pandas.Timestamp(ref_date) + pandas.DateOffset(months=-6)).strftime('%Y-%m-%d')
    financials = mdb_query.get_financials(symbols, sixMonthsBeforeDate, "after")
    financials = financials[ financials['reportDate'] <= earnings['fiscalEndDate'].max() ]
    financials = financials[["symbol","reportDate","netIncome","shareholderEquity"]]
    #print( financials )
    #Get prices for inception date
    print( "Query prices" )
    idx_min = 0
    query_num = 100
    prices = pandas.DataFrame()
    while idx_min < len(symbols):
        idx_max = idx_min + query_num
        if idx_max > len(symbols):
            idx_max = len(symbols)
        symbols_split = symbols[idx_min:idx_max]
        prices_split = mdb_query.get_chart(symbols_split, ref_date, "latest")
        prices = prices.append(prices_split, ignore_index=True, sort=False)
        idx_min = idx_min + query_num
    prices.reset_index(drop=True, inplace=True)
    #print( prices )
    #Get company data
    company = mdb_query.get_company( symbols )
    company = company[['symbol','companyName','industry','sector']]
    #Merge dataframes together
    print( "Merge dataframes" )
    merged = pandas.merge(earnings,financials,how='inner',left_on=["symbol","fiscalEndDate"],right_on=["symbol","reportDate"],sort=False)
    merged = pandas.merge(merged,prices,how='inner',on="symbol",sort=False)
    merged = pandas.merge(merged,company,how='inner',on='symbol',sort=False)
    #Remove any rows with missing values
    merged = merged.dropna(axis=0, subset=["netIncome","actualEPS","close","shareholderEquity"])
    #Calculate marketCap value
    # price * netIncome / EPS = price * sharesOutstanding = mcap
    # Actually not 100% accurate, should be netIncome - preferred dividend
    # Doesn't perfectly match IEX value or google - probably good enough
    merged["sharesOutstanding"] = merged.netIncome / merged.actualEPS
    merged["marketCap"] = merged.sharesOutstanding * merged.close
    #Calculate PE, ROE, and ratio
    merged["peRatio"] = merged.close / merged.actualEPS
    merged["returnOnEquity"] = merged.netIncome / merged.shareholderEquity
    merged["peROERatio"] = merged.peRatio / merged.returnOnEquity
    #Count number of stocks above mcap value
    # A useful indicator of how universe compares to S&P500
    print( "Universe before cuts..." )
    print( "mcap > 50M: " + str(merged[merged["marketCap"] > 50000000].count()["marketCap"]) )
    print( "mcap > 100M: " + str(merged[merged["marketCap"] > 100000000].count()["marketCap"]) )
    print( "mcap > 500M: " + str(merged[merged["marketCap"] > 500000000].count()["marketCap"]) )
    print( "mcap > 1B: " + str(merged[merged["marketCap"] > 1000000000].count()["marketCap"]) )
    print( "mcap > 5B: " + str(merged[merged["marketCap"] > 5000000000].count()["marketCap"]) )
    print( "mcap > 10B: " + str(merged[merged["marketCap"] > 10000000000].count()["marketCap"]) )
    print( "mcap > 50B: " + str(merged[merged["marketCap"] > 50000000000].count()["marketCap"]) )
    print( "mcap > 100B: " + str(merged[merged["marketCap"] > 100000000000].count()["marketCap"]) )
    #Rank stocks
    #Cut negative PE and ROE
    merged = merged[(merged.peRatio > 0) & (merged.returnOnEquity > 0)]
    #Remove invalid stock symbols, and different voting options
    # Do the different voting options affect marketCap?
    #forbidden = [ "#", ".", "-" ]
    #merged = merged[ merged.apply( lambda x: not any( s in x['symbol'] for s in forbidden ), axis=1 ) ]
    #Remove American Depositary Shares
    #ads_str = 'American Depositary Shares'
    #merged = merged[ merged.apply( lambda x: ads_str not in x['companyName'], axis=1 ) ]
    #Remove industries that do not compare well
    # e.g. Companies that have investments as assets
    #forbidden_industry = ['Brokers & Exchanges','REITs','Asset Management','Banks']
    #merged = merged[ ~merged.industry.isin( forbidden_industry ) ]
    #Count number of stocks after cuts
    print( "Universe after cuts..." )
    print( "mcap > 50M: " + str(merged[merged["marketCap"] > 50000000].count()["marketCap"]) )
    print( "mcap > 100M: " + str(merged[merged["marketCap"] > 100000000].count()["marketCap"]) )
    print( "mcap > 500M: " + str(merged[merged["marketCap"] > 500000000].count()["marketCap"]) )
    print( "mcap > 1B: " + str(merged[merged["marketCap"] > 1000000000].count()["marketCap"]) )
    print( "mcap > 5B: " + str(merged[merged["marketCap"] > 5000000000].count()["marketCap"]) )
    print( "mcap > 10B: " + str(merged[merged["marketCap"] > 10000000000].count()["marketCap"]) )
    print( "mcap > 50B: " + str(merged[merged["marketCap"] > 50000000000].count()["marketCap"]) )
    print( "mcap > 100B: " + str(merged[merged["marketCap"] > 100000000000].count()["marketCap"]) )
    #Order by peROERatio
    merged = merged.sort_values(by="peROERatio", ascending=True, axis="index")

    return merged

def calculate_top_stocks(ref_date):
    """
    Calculate ranked list of stocks
    @params:
        ref_date    - Required  : date YYYY-MM-DD (Str)
    """

    #Get ranked stock list for given date
    symbols = mdb_query.get_active_companies().tolist()
    print( "Query balance sheets" )
    balancesheets = mdb_query.get_balancesheets(symbols, ref_date, "latest")
    #earnings = earnings[["EPSReportDate","actualEPS","fiscalEndDate","fiscalPeriod","symbol"]]
    #print( earnings )
    #Get financials within 6 months
    #print( "Query financials" )
    sixMonthsBeforeDate = (pandas.Timestamp(ref_date) + pandas.DateOffset(months=-6)).strftime('%Y-%m-%d')
    #financials = mdb_query.get_financials(symbols, sixMonthsBeforeDate, "after")
    balancesheets = balancesheets[ balancesheets['reportDate'] >= sixMonthsBeforeDate ]
    #financials = financials[["symbol","reportDate","netIncome","shareholderEquity"]]
    #print( financials )
    #Get prices for inception date
    print( "Query prices" )
    idx_min = 0
    query_num = 100
    prices = pandas.DataFrame()
    while idx_min < len(symbols):
        idx_max = idx_min + query_num
        if idx_max > len(symbols):
            idx_max = len(symbols)
        symbols_split = symbols[idx_min:idx_max]
        prices_split = mdb_query.get_quotes(symbols_split, ref_date, "latest")
        prices = prices.append(prices_split, ignore_index=True, sort=False)
        idx_min = idx_min + query_num
    #Get prices within 7 days
    fiveDaysBeforeDate = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-7)).strftime('%Y-%m-%d')
    prices = prices[ prices['date'] >= fiveDaysBeforeDate ]
    prices.reset_index(drop=True, inplace=True)
    #print( prices )
    #Get company data
    company = mdb_query.get_company( symbols )
    company = company[['symbol','companyName']]
    #Merge dataframes together
    print( "Merge dataframes" )
    #merged = pandas.merge(earnings,financials,how='inner',left_on=["symbol","fiscalEndDate"],right_on=["symbol","reportDate"],sort=False)
    merged = pandas.merge(balancesheets,prices,how='inner',on="symbol",sort=False)
    merged = pandas.merge(merged,company,how='inner',on='symbol',sort=False)
    #Remove any rows with missing values
    merged = merged.dropna(axis=0, subset=['shareholderEquity','close','marketCap','peRatio'])
    #Calculate ROE
    #close / peRatio = EPS
    #marketCap / close = sharesOutstanding
    #sharesOutstanding * EPS = netIncome
    #netIncome / shareholderEquity = returnOnEquity
    merged = merged[ merged.peRatio != 0 ]
    merged["EPS"] = merged.close / merged.peRatio
    merged = merged[ merged.close != 0 ]
    merged["sharesOutstanding"] = merged.marketCap / merged.close
    merged["netIncome"] = merged.sharesOutstanding * merged.EPS
    merged = merged[ merged.shareholderEquity != 0 ]
    merged["returnOnEquity"] = merged.netIncome / merged.shareholderEquity
    merged = merged[ merged.returnOnEquity != 0 ]
    merged["peROERatio"] = merged.peRatio / merged.returnOnEquity
    #Count number of stocks above mcap value
    # A useful indicator of how universe compares to S&P500
    print( "Universe before cuts..." )
    print( "mcap > 50M: " + str(merged[merged["marketCap"] > 50000000].count()["marketCap"]) )
    print( "mcap > 100M: " + str(merged[merged["marketCap"] > 100000000].count()["marketCap"]) )
    print( "mcap > 500M: " + str(merged[merged["marketCap"] > 500000000].count()["marketCap"]) )
    print( "mcap > 1B: " + str(merged[merged["marketCap"] > 1000000000].count()["marketCap"]) )
    print( "mcap > 5B: " + str(merged[merged["marketCap"] > 5000000000].count()["marketCap"]) )
    print( "mcap > 10B: " + str(merged[merged["marketCap"] > 10000000000].count()["marketCap"]) )
    print( "mcap > 50B: " + str(merged[merged["marketCap"] > 50000000000].count()["marketCap"]) )
    print( "mcap > 100B: " + str(merged[merged["marketCap"] > 100000000000].count()["marketCap"]) )
    #Rank stocks
    #Cut negative PE and ROE
    merged = merged[(merged.peRatio > 0) & (merged.returnOnEquity > 0)]
    #Remove invalid stock symbols, and different voting options
    # Do the different voting options affect marketCap?
    #forbidden = [ "#", ".", "-" ]
    #merged = merged[ merged.apply( lambda x: not any( s in x['symbol'] for s in forbidden ), axis=1 ) ]
    #Remove American Depositary Shares
    #ads_str = 'American Depositary Shares'
    #merged = merged[ merged.apply( lambda x: ads_str not in x['companyName'], axis=1 ) ]
    #Remove industries that do not compare well
    # e.g. Companies that have investments as assets
    #forbidden_industry = ['Brokers & Exchanges','REITs','Asset Management','Banks']
    #merged = merged[ ~merged.industry.isin( forbidden_industry ) ]
    #Count number of stocks after cuts
    print( "Universe after cuts..." )
    print( "mcap > 50M: " + str(merged[merged["marketCap"] > 50000000].count()["marketCap"]) )
    print( "mcap > 100M: " + str(merged[merged["marketCap"] > 100000000].count()["marketCap"]) )
    print( "mcap > 500M: " + str(merged[merged["marketCap"] > 500000000].count()["marketCap"]) )
    print( "mcap > 1B: " + str(merged[merged["marketCap"] > 1000000000].count()["marketCap"]) )
    print( "mcap > 5B: " + str(merged[merged["marketCap"] > 5000000000].count()["marketCap"]) )
    print( "mcap > 10B: " + str(merged[merged["marketCap"] > 10000000000].count()["marketCap"]) )
    print( "mcap > 50B: " + str(merged[merged["marketCap"] > 50000000000].count()["marketCap"]) )
    print( "mcap > 100B: " + str(merged[merged["marketCap"] > 100000000000].count()["marketCap"]) )
    #Order by peROERatio
    merged = merged.sort_values(by="peROERatio", ascending=True, axis="index")

    return merged
