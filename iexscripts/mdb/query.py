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

class Query(Mdb):
    def __init__(self):
        #Inherit all methods and properties from Mdb
        super().__init__()

    def is_new_symbol(self, symbol):
        """
        Is symbol already in MongoDB?
        @params:
            symbol  - Required  : symbol (Dataframe)
        """
    
        query = { "symbol": symbol["symbol"] }
    
        results = self.db.iex_symbols.find( query ).sort("date", ASCENDING)
    
        entry_match = False
    
        #Does most recent entry in mongoDB match IEX?
        for doc in results:
            if (doc.get("iexId") == symbol["iexId"] and
                doc.get("isEnabled") == symbol["isEnabled"] and
                doc.get("name") == symbol["name"] and
                doc.get("type") == symbol["type"]):
                entry_match = True
            else:
                entry_match = False
    
        return not entry_match
    
    def get_symbols(self):
        """
        Return symbols from MongoDB
        """
    
        results = self.db.iex_symbols.aggregate([
            { "$sort": { "date": DESCENDING } },
            { "$group": {
                "_id": "$symbol",
                "symbols": { "$push": "$$ROOT" }
                }
            },
            { "$replaceRoot": {
                "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                }
            },
            { "$sort": { "symbol": ASCENDING } }
        ])
    
        symbols = pandas.DataFrame()
        for doc in results:
            symbols = symbols.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        if not symbols.empty:
            symbols.drop("_id", axis=1, inplace=True)
            symbols = symbols[symbols.isEnabled != False]
            symbols.reset_index(drop=True, inplace=True)
    
        return symbols
    
    def get_company(self, symbol):
        """
        Return company information from MongoDB
        @params:
            symbol  - Required  : symbol list ([Str])
        """
    
        query = { "symbol": { "$in": symbol } }
    
        results = self.db.iex_company.find( query ).sort("symbol", ASCENDING)
    
        company = pandas.DataFrame()
        for doc in results:
            company = company.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        company.drop("_id", axis=1, errors='ignore', inplace=True)
        company.reset_index(drop=True, inplace=True)
    
        return company
    
    def get_active_companies(self):
    
        symbols = self.get_symbols()
        companies = self.get_company( symbols["symbol"].tolist() )
        symbols = pandas.merge( symbols, companies, how='inner', left_on=['symbol','type'], right_on=['symbol','issueType'], sort=False)
        symbols_spy = symbols[ symbols['symbol'] == 'SPY' ]
        #type="cs"
        symbols = symbols[ symbols.type.isin( ['cs'] ) ]
        #region="US"
        symbols = symbols[ symbols.region.isin( ['US'] ) ]
        #currency="USD"
        symbols = symbols[ symbols.currency.isin( ['USD'] ) ]
        #isEnabled=True
        symbols = symbols[ symbols['isEnabled'] == True ]
        #exchange not in NASDAQ or NYSE
        symbols = symbols[ symbols.exchange_x.isin( ['NAS','NYS'] ) ]
        #Class B etc. not in securityName
        forbidden = [ 'Class B', 'Class C', 'Class D', 'Class E', 'Class F' ]
        symbols = symbols[ symbols.apply( lambda x: not any( s in x['securityName'] for s in forbidden ), axis=1 ) ]
        forbidden = [ "#", ".", "-" ]
        symbols = symbols[ symbols.apply( lambda x: not any( s in x['symbol'] for s in forbidden ), axis=1 ) ]
        #Remove American Depositary Shares
        #Not in IEX anymore?
        #ads_str = 'American Depositary Shares'
        #symbols = symbols[ symbols.apply( lambda x: ads_str not in x['name'], axis=1 ) ]
        #symbols = symbols[ symbols.apply( lambda x: ads_str not in x['companyName'], axis=1 ) ]
        #symbols = symbols[ symbols.apply( lambda x: ads_str not in x['securityName'], axis=1 ) ]
        #Remove industries that do not compare well
        # e.g. Companies that have investments as assets
        forbidden_industry = ['Investment Managers','Real Estate Investment Trusts','Regional Banks','Financial Conglomerates','Major Banks','Investment Banks/Brokers','Savings Banks','Investment Trusts/Mutual Funds','Financial Publishing/Services']
        symbols = symbols[ ~symbols.industry.isin( forbidden_industry ) ]
        symbols = symbols.append(symbols_spy, ignore_index=True, sort=False)
        symbols.reset_index(drop=True, inplace=True)
        
        return symbols['symbol']
    
    def get_chart(self, ref_symbol, ref_date = "1990-01-01", when = "after"):
        """
        Return company charts from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, on, latest (Str)
        """
    
        query = []
    
        #No more than 10 days ago
        gte_date = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-10)).strftime('%Y-%m-%d')
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "date": { "$gte": ref_date } }
        elif when == "on":
            query = { "symbol": { "$in": ref_symbol },
                        "date": ref_date }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "date": { "$lte": ref_date } } },
                        { "$match": { "date": { "$gte": gte_date } } },
                        { "$sort": { "date": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
            #query = { "symbol": { "$in": ref_symbol },
            #          "date": { "$lte": ref_date },
            #          "date": { "$gte": gte_date } }
        else:
            sys.exit("when not in [after, on, latest]!")
    
        results = []
    
        if when == "after" or when == "on":
            results = self.db.iex_charts.find( query ).sort("date", DESCENDING)
        else:
            results = self.db.iex_charts.aggregate( query )
            #results = self.db.iex_charts.find( query )
    
        chart = pandas.DataFrame()
        for doc in results:
            #chart = chart.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
            chart = pandas.concat( [chart,pandas.DataFrame.from_dict(doc, orient='index').T], axis=0, ignore_index=True, sort=False )
    
        #if when == "latest":
        #    chart = chart.loc[chart.groupby("symbol").date.idxmax(),:]
        #    chart.sort_values("symbol", axis=0, ascending=True, inplace=True)
    
        chart.drop(["_id","open","uClose","uHigh","uLow","uOpen","uVolume"], axis=1, errors='ignore', inplace=True)
        chart.reset_index(drop=True, inplace=True)
    
        return chart
    
    def get_quotes(self, ref_symbol, ref_date = "1990-01-01", when = "after"):
        """
        Return company quotes from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, on, latest (Str)
        """
    
        query = []
    
        #No more than 10 days ago
        gte_date = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-10)).strftime('%Y-%m-%d')
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "date": { "$gte": ref_date } }
        elif when == "on":
            query = { "symbol": { "$in": ref_symbol },
                        "date": ref_date }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "date": { "$lte": ref_date } } },
                        { "$match": { "date": { "$gte": gte_date } } },
                        { "$sort": { "date": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
            #query = { "symbol": { "$in": ref_symbol },
            #          "date": { "$lte": ref_date },
            #          "date": { "$gte": gte_date } }
        else:
            sys.exit("when not in [after, on, latest]!")
    
        results = []
    
        if when == "after" or when == "on":
            results = self.db.iex_quotes.find( query ).sort("date", DESCENDING)
        else:
            results = self.db.iex_quotes.aggregate( query )
            #results = self.db.iex_quotes.find( query )
    
        quote = pandas.DataFrame()
        for doc in results:
            quote = pandas.concat( [quote,pandas.DataFrame.from_dict(doc, orient='index').T], axis=0, ignore_index=True, sort=False )
    
        quote.drop(["_id"], axis=1, errors='ignore', inplace=True)
        quote.reset_index(drop=True, inplace=True)
    
        return quote
    
    def get_dividends(self, ref_symbol, ref_date = "1900-01-01", when = "after"):
        """
        Return company dividends from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, on, latest (Str)
        """
    
        query = []
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "exDate": { "$gte": ref_date } }
        elif when == "on":
            query = { "symbol": { "$in": ref_symbol },
                        "exDate": ref_date }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "exDate": { "$lte": ref_date } } },
                        { "$sort": { "exDate": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [after, latest]")
    
        results = []
    
        if when == "after" or when == "on":
            results = self.db.iex_dividends.find( query ).sort("exDate", DESCENDING)
        else:
            results = self.db.iex_dividends.aggregate( query )
    
        dividends = pandas.DataFrame()
        for doc in results:
            dividends = dividends.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        dividends.drop("_id", axis=1, errors='ignore', inplace=True)
        dividends.reset_index(drop=True, inplace=True)
    
        return dividends
    
    def get_earnings(self, ref_symbol, ref_date = "1900-01-01", when = "after", date_type = "fiscalEndDate"):
        """
        Return company earnings from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, latest (Str)
            date_type   - Optional  : fiscalEndDate, EPSReportDate (Str)
        """
    
        query = []
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        date_type: { "$gte": ref_date } }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        date_type: { "$lte": ref_date } } },
                        { "$sort": { date_type: DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [after, latest]")
    
        results = []
    
        if when == "after":
            results = self.db.iex_earnings.find( query ).sort(date_type, DESCENDING)
        else:
            results = self.db.iex_earnings.aggregate( query )
    
        earnings = pandas.DataFrame()
        for doc in results:
            earnings = earnings.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        earnings.drop("_id", axis=1, errors='ignore', inplace=True)
        earnings.reset_index(drop=True, inplace=True)
    
        return earnings
    
    def get_financials(self, ref_symbol, ref_date = "1900-01-01", when = "after"):
        """
        Return company financials from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, latest (Str)
        """
    
        query = []
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "reportDate": { "$gte": ref_date } }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "reportDate": { "$lte": ref_date } } },
                        { "$sort": { "reportDate": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [after, latest]")
    
        results = []
    
        if when == "after":
            results = self.db.iex_financials.find( query ).sort("reportDate", DESCENDING)
        else:
            results = self.db.iex_financials.aggregate( query )
    
        financials = pandas.DataFrame()
        for doc in results:
            financials = financials.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        financials.drop("_id", axis=1, errors='ignore', inplace=True)
        financials.reset_index(drop=True, inplace=True)
    
        return financials
    
    def get_balancesheets(self, ref_symbol, ref_date = "1900-01-01", when = "after"):
        """
        Return company balance sheet from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, latest (Str)
        """
    
        query = []
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "reportDate": { "$gte": ref_date } }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "reportDate": { "$lte": ref_date } } },
                        { "$sort": { "reportDate": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [after, latest]")
    
        results = []
    
        if when == "after":
            results = self.db.iex_balancesheets.find( query ).sort("reportDate", DESCENDING)
        else:
            results = self.db.iex_balancesheets.aggregate( query )
    
        balancesheet = pandas.DataFrame()
        for doc in results:
            balancesheet = balancesheet.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        balancesheet.drop("_id", axis=1, errors='ignore', inplace=True)
        balancesheet.reset_index(drop=True, inplace=True)
    
        return balancesheet
    
    def get_stats(self, ref_symbol, ref_date = "1900-01-01", when = "after"):
        """
        Return company balance sheet from MongoDB
        @params:
            ref_symbol  - Required  : symbol list ([Str])
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : after, latest (Str)
        """
    
        query = []
    
        if when == "after":
            query = { "symbol": { "$in": ref_symbol },
                        "date": { "$gte": ref_date } }
        elif when == "latest":
            query = [
                        { "$match": { "symbol": { "$in": ref_symbol },
                                        "date": { "$lte": ref_date } } },
                        { "$sort": { "date": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [after, latest]")
    
        results = []
    
        if when == "after":
            results = self.db.iex_stats.find( query ).sort("date", DESCENDING)
        else:
            results = self.db.iex_stats.aggregate( query )
    
        stats = pandas.DataFrame()
        for doc in results:
            stats = stats.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        stats.drop("_id", axis=1, errors='ignore', inplace=True)
        stats.reset_index(drop=True, inplace=True)
    
        return stats
    
    def get_portfolios(self, date):
        """
        Return portfolio information from MongoDB
        @params:
            date    - Required  : date YYYY-MM-DD (Str)
        """
    
        query = { "inceptionDate": { "$lte": date } }
    
        results = self.db.pf_info.find( query ).sort("portfolioID", ASCENDING)
    
        portfolios = pandas.DataFrame()
        for doc in results:
            portfolios = portfolios.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        portfolios.drop("_id", axis=1, inplace=True)
    
        return portfolios
    
    def get_transactions(self, portfolioID, date, when = "on"):
        """
        Return portfolio transactions from MongoDB
        @params:
            portfolioID - Required  : portfolio ID (Str)
            date        - Required  : date YYYY-MM-DD (Str)
            when        - Optional  : on, after (Str)
        """
    
        query = {}
    
        if when == "on":
            query = { "portfolioID": portfolioID,
                        "date": date }
        elif when == "after":
            query = { "portfolioID": portfolioID,
                        "date": { "$gte": date } }
        else:
            sys.exit("when not in [on, after]")
    
        results = self.db.pf_transactions.find( query )
    
        transactions = pandas.DataFrame()
        for doc in results:
            transactions = transactions.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        if not transactions.empty:
            transactions.drop("_id", axis=1, inplace=True)
    
        return transactions
    
    def get_holdings(self, portfolioID, date = " 1990-01-01", when = "on"):
        """
        Return portfolio holdings from MongoDB
        @params:
            portfolioID - Required  : portfolio ID (Str)
            date        - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, after (Str)
        """
    
        query = []
    
        if when == "on":
            query = [
                        { "$match": { "portfolioID": portfolioID,
                                        "lastUpdated": { "$lte": date } } },
                        { "$sort": { "lastUpdated": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        elif when == "after":
            query = { "portfolioID": portfolioID,
                        "lastUpdated": { "$gte": date } }
        else:
            sys.exit("when not in [on, after]")
    
        results = []
    
        if when == "on":
            results = self.db.pf_holdings.aggregate( query )
        else:
            results = self.db.pf_holdings.find( query ).sort("date", ASCENDING)
    
        holdings = pandas.DataFrame()
        for doc in results:
            holdings = holdings.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        if not holdings.empty:
            holdings.drop("_id", axis=1, inplace=True)
    
        return holdings
    
    def get_performance(self, ref_portfolioID, ref_date = "1990-01-01"):
        """
        Return portfolio performance from MongoDB
        @params:
            ref_portfolioID - Required  : portfolio ID (Str)
            ref_date        - Optional  : date YYYY-MM-DD (Str)
        """
    
        query = { "portfolioID": { "$in": ref_portfolioID },
                    "date": { "$gte": ref_date } }
    
        results = self.db.pf_performance.find( query ).sort("date", DESCENDING)
       
        performance = pandas.DataFrame()
        for doc in results:
            performance = performance.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        performance.drop("_id", axis=1, errors='ignore', inplace=True)
        performance.reset_index(drop=True, inplace=True)
    
        return performance
    
    def get_stock_list(self, ref_date = "1990-01-01", when = "on"):
        """
        Return ranked list of stocks from MongoDB
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, latest (Str)
        """
    
        query = []
    
        gte_date = (pandas.Timestamp(ref_date) + pandas.DateOffset(days=-50)).strftime('%Y-%m-%d')
    
        if when == "on":
            query = { "date": ref_date }
        elif when == "latest":
            query = [
                        { "$match": { "date": { "$lte": ref_date } } },
                        { "$match": { "date": { "$gte": gte_date } } },
                        { "$sort": { "date": DESCENDING } },
                        { "$group": {
                            "_id": "$symbol",
                            "symbols": { "$push": "$$ROOT" }
                            }
                        },
                        { "$replaceRoot": {
                            "newRoot": { "$arrayElemAt": ["$symbols", 0] }
                            }
                        },
                        { "$sort": { "symbol": ASCENDING } }
                    ]
        else:
            sys.exit("when not in [on, latest]")
    
        results = []
    
        if when == "on":
            results = self.db.stock_list.find( query )
        elif when == "latest":
            results = self.db.stock_list.aggregate( query )
    
        stock_list = pandas.DataFrame()
        for doc in results:
            stock_list = stock_list.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
    
        if not stock_list.empty:
            stock_list = stock_list.sort_values(by="peROERatio", ascending=True, axis="index")
        stock_list.drop("_id", axis=1, errors='ignore', inplace=True)
        stock_list.reset_index(drop=True, inplace=True)
    
        return stock_list
