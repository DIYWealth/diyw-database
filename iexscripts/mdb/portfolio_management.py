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
from mdb.mdb import Mdb
from mdb.query import Query
from mdb.algo import Algo

class PortfolioManagement(Mdb):
    def __init__(self):
        #Inherit all methods and properties from Mdb
        super().__init__()

    #For a given date find the top ranked stocks
    #Insert tables defining the portfolios
    #Insert transactions to deposit cash
    #Insert transactions to buy top ranked stocks
    def insert_portfolio(self):
        #Build portfolios for 100M, 500M, 1B, 5B, 10B, 50B mcap stocks
        #Insert tables describing the portfolios
        portfolio_tables = [
                            { "portfolioID": "stocks30mcap100M",
                                "name": "30 stocks, 100M market cap",
                                "description": "Portfolio of 30 stocks above 100M market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 100000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks30mcap500M",
                                "name": "30 stocks, 500M market cap",
                                "description": "Portfolio of 30 stocks above 500M market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 500000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks30mcap1B",
                                "name": "30 stocks, 1B market cap",
                                "description": "Portfolio of 30 stocks above 1B market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 1000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks30mcap5B",
                                "name": "30 stocks, 5B market cap",
                                "description": "Portfolio of 30 stocks above 5B market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 5000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks30mcap10B",
                                "name": "30 stocks, 10B market cap",
                                "description": "Portfolio of 30 stocks above 10B market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 10000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks30mcap50B",
                                "name": "30 stocks, 50B market cap",
                                "description": "Portfolio of 30 stocks above 50B market cap with initial investment of 100M USD",
                                "nStocks": 30,
                                "mcapMinimum": 50000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap100M",
                                "name": "50 stocks, 100M market cap",
                                "description": "Portfolio of 50 stocks above 100M market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 100000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap500M",
                                "name": "50 stocks, 500M market cap",
                                "description": "Portfolio of 50 stocks above 500M market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 500000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap1B",
                                "name": "50 stocks, 1B market cap",
                                "description": "Portfolio of 50 stocks above 1B market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 1000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap5B",
                                "name": "50 stocks, 5B market cap",
                                "description": "Portfolio of 50 stocks above 5B market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 5000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap10B",
                                "name": "50 stocks, 10B market cap",
                                "description": "Portfolio of 50 stocks above 10B market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 10000000000,
                                "inceptionDate": "2018-07-02" },
                            { "portfolioID": "stocks50mcap50B",
                                "name": "50 stocks, 50B market cap",
                                "description": "Portfolio of 50 stocks above 50B market cap with initial investment of 100M USD",
                                "nStocks": 50,
                                "mcapMinimum": 50000000000,
                                "inceptionDate": "2018-07-02" }
                            ]
        insert_pf_info = True
        if insert_pf_info:
            print( "Inserting portfolio tables" )
            self.db.pf_info.insert_many( portfolio_tables )
    
    def insert_transactions(self):
        print( "Create portfolio transaction tables" )
        transactionDate = "2018-07-02"
        dayBeforeDate = (pandas.Timestamp(transactionDate) + pandas.DateOffset(days=-1)).strftime('%Y-%m-%d')
        print( dayBeforeDate )
        #Get ranked stock list for current date
        mdb_algo = Algo()
        merged = mdb_algo.calculate_top_stocks(dayBeforeDate)
        #Define dataframes containing stocks to be bought
        stocks30mcap100M = merged[merged["marketCap"] > 100000000].head(30).reset_index(drop=True)
        stocks30mcap500M = merged[merged["marketCap"] > 500000000].head(30).reset_index(drop=True)
        stocks30mcap1B = merged[merged["marketCap"] > 1000000000].head(30).reset_index(drop=True)
        stocks30mcap5B = merged[merged["marketCap"] > 5000000000].head(30).reset_index(drop=True)
        stocks30mcap10B = merged[merged["marketCap"] > 10000000000].head(30).reset_index(drop=True)
        stocks30mcap50B = merged[merged["marketCap"] > 50000000000].head(30).reset_index(drop=True)
        stocks50mcap100M = merged[merged["marketCap"] > 100000000].head(50).reset_index(drop=True)
        stocks50mcap500M = merged[merged["marketCap"] > 500000000].head(50).reset_index(drop=True)
        stocks50mcap1B = merged[merged["marketCap"] > 1000000000].head(50).reset_index(drop=True)
        stocks50mcap5B = merged[merged["marketCap"] > 5000000000].head(50).reset_index(drop=True)
        stocks50mcap10B = merged[merged["marketCap"] > 10000000000].head(50).reset_index(drop=True)
        stocks50mcap50B = merged[merged["marketCap"] > 50000000000].head(50).reset_index(drop=True)
        stocks30mcap100M["portfolioID"] = "stocks30mcap100M"
        stocks30mcap500M["portfolioID"] = "stocks30mcap500M"
        stocks30mcap1B["portfolioID"] = "stocks30mcap1B"
        stocks30mcap5B["portfolioID"] = "stocks30mcap5B"
        stocks30mcap10B["portfolioID"] = "stocks30mcap10B"
        stocks30mcap50B["portfolioID"] = "stocks30mcap50B"
        stocks50mcap100M["portfolioID"] = "stocks50mcap100M"
        stocks50mcap500M["portfolioID"] = "stocks50mcap500M"
        stocks50mcap1B["portfolioID"] = "stocks50mcap1B"
        stocks50mcap5B["portfolioID"] = "stocks50mcap5B"
        stocks50mcap10B["portfolioID"] = "stocks50mcap10B"
        stocks50mcap50B["portfolioID"] = "stocks50mcap50B"
        portfolio_dfs = [ stocks30mcap100M, stocks30mcap500M, stocks30mcap1B, stocks30mcap5B, stocks30mcap10B, stocks30mcap50B, stocks50mcap100M, stocks50mcap500M, stocks50mcap1B, stocks50mcap5B, stocks50mcap10B, stocks50mcap50B ]
        #Build transaction tables to deposit cash into the portfolio
        transaction_tables = []
        for portfolio_df in portfolio_dfs:
            transaction_table = { "portfolioID": portfolio_df.iloc[0]["portfolioID"],
                                    "symbol": "USD",
                                    "type": "deposit",
                                    "date": "2018-07-02",
                                    "price": 1.0,
                                    "volume": 100000000,
                                    "commission": 0.0 }
            transaction_tables.append( transaction_table )
        insert_pf_transactions = True
        if insert_pf_transactions:
            self.db.pf_transactions.insert_many( transaction_tables )
        #Build transaction tables which buy the stocks
        transaction_tables = []
        #Loop through portfolio dataframes
        for portfolio_df in portfolio_dfs:
            #Loop through stocks to be purchased
            for index, stock in portfolio_df.iterrows():
                #Calculate volume of stock to be purchased
                #(Deposit/No. of stocks)/Price rounded to integer
                volume = 100000000
                #Divide by number of stocks to be purchased
                volume = volume / len(portfolio_df.index)
                #if "stocks30" in portfolio_df.iloc[0]["portfolioID"]:
                #    volume = volume / 30
                #elif "stocks50" in portfolio_df.iloc[0]["portfolioID"]:
                #    volume = volume / 50
                volume = volume / stock.close
                volume = round(volume)
                transaction_table = { "portfolioID": portfolio_df.iloc[0]["portfolioID"],
                                        "symbol": stock.symbol,
                                        "type": "buy",
                                        "date": "2018-07-02",
                                        "price": stock.close,
                                        "volume": volume,
                                        "commission": 0.0 }
                transaction_tables.append( transaction_table )
        insert_pf_transactions = True
        if insert_pf_transactions:
            self.db.pf_transactions.insert_many( transaction_tables )
    
    def pf_sell_all(self, ref_date = "1990-01-01"):
        """
        Sell all holdings on a particular date
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
        """
        mdb_query = Query()
        #Get link to MongoDB
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
                self.db.pf_transactions.insert_many( transaction_tables )
    
    def pf_buy_all(self, ref_date = "1990-01-01"):
        """
        Buy all stocks in the top stocks list on a particular date
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
        """
        mdb_query = Query()
        #Get link to MongoDB
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
                self.db.pf_transactions.insert_many( transaction_tables )
