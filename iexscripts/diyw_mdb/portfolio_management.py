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
from iexscripts.utils import printProgressBar
from iexscripts.diyw_mdb.connection import get_mongodb
import iexscripts.diyw_mdb.query as mdb_query

def pf_sell_all(ref_date = "1990-01-01"):
    """
    Sell all holdings on a particular date
    @params:
        ref_date    - Optional  : date YYYY-MM-DD (Str)
    """
    #Get link to MongoDB
    db = get_mongodb()
    #Get date for stock prices
    dayBeforeDate = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-1)).strftime('%Y-%m-%d')
    #Get existing portfolios
    portfolios = mdb_query.get_portfolios(ref_date)[["portfolioID","inceptionDate"]]
    #Loop through portfolios
    for portfolio_index, portfolio_row in portfolios.iterrows():
        #Get portfolioID and inceptionDate
        portfolio = portfolio_row['portfolioID']
        #Get holdings table
        holdings = mdb_query.get_holdings(portfolio, ref_date, "on")
        #Get latest prices from dayBeforeDate
        prices = mdb_query.get_chart(holdings['symbol'].tolist(), dayBeforeDate, 'latest')
        #Merge prices with holdings
        holdings = pandas.merge(holdings,prices,how='left',left_on=['symbol'],right_on=['symbol'],sort=False)
        #Remove USD
        holdings = holdings[ holdings['symbol'] != 'USD' ]
        #print( holdings )
        #Build transaction tables which sell the stocks
        transaction_tables = []
        #Loop through stocks to be sold
        for index, stock in holdings.iterrows():
        #Transaction tables: type = sell, date = ref_date, price = close, volume = endOfDayQuantity
            transaction_table = { "portfolioID": portfolio_row.portfolioID,
                                  "symbol": stock.symbol,
                                  "type": "sell",
                                  "date": ref_date,
                                  "price": stock.close,
                                  "volume": stock.endOfDayQuantity,
                                  "commission": 0.0 }
            transaction_tables.append( transaction_table )
        insert_pf_transactions = True
        if insert_pf_transactions:
            #print( transaction_tables )
            db.pf_transactions.insert_many( transaction_tables )

def pf_buy_all(ref_date = "1990-01-01"):
    """
    Buy all stocks in the top stocks list on a particular date
    @params:
        ref_date    - Optional  : date YYYY-MM-DD (Str)
    """
    #Get link to MongoDB
    db = get_mongodb()
    #Get date for stock prices
    dayBeforeDate = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-1)).strftime('%Y-%m-%d')
    #Get ranked stock list for current date
    top_stocks_full = calculate_top_stocks(dayBeforeDate)
    #Get existing portfolios
    portfolios = mdb_query.get_portfolios(ref_date)
    #Loop through portfolios
    for portfolio_index, portfolio_row in portfolios.iterrows():
        #Get holdings table
        holdings = mdb_query.get_holdings(portfolio_row.portfolioID, ref_date, "on")
        holdings_usd = holdings[ holdings['symbol'] == 'USD' ]
        holdings_usd.reset_index(drop=True, inplace=True)
        top_stocks = top_stocks_full[ top_stocks_full['marketCap'] > portfolio_row.mcapMinimum ].head( portfolio_row.nStocks ).reset_index(drop=True)
        transaction_tables = []
        #Loop through stocks to be purchased
        for index, stock in top_stocks.iterrows():
            #Calculate volume of stock to be purchased
            #(Current USD holding/No. of stocks)/Price rounded to integer
            volume = holdings_usd.endOfDayQuantity.iloc[0]
            volume = volume / portfolio_row.nStocks
            volume = volume / stock.close
            volume = round(volume)
            transaction_table = { "portfolioID": portfolio_row.portfolioID,
                                    "symbol": stock.symbol,
                                    "type": "buy",
                                    "date": ref_date,
                                    "price": stock.close,
                                    "volume": volume,
                                    "commission": 0.0 }
            transaction_tables.append( transaction_table )
        insert_pf_transactions = True
        if insert_pf_transactions:
            db.pf_transactions.insert_many( transaction_tables )
