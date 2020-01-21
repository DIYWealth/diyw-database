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

class Delete(Mdb):
    def __init__(self):
        #Inherit all methods and properties from Mdb
        super().__init__()

    def delete_duplicate_charts(self, ref_date = "1990-01-01", when = "on"):
        """
        Delete duplicates prices in MongoDB
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, latest (Str)
        """
    
        mdb_query = Query()
        mdb_symbols = mdb_query.get_symbols()
        #mdb_symbols = mdb_symbols.iloc[ 999: , : ]
        mdb_symbols.reset_index(drop=True, inplace=True)
        #mdb_symbols = ["A"]
        #print( mdb_symbols )
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        for index, symbol in mdb_symbols.iterrows():
            #if index > 10:
            #    break
            #print( symbol )
            query = { "symbol": { "$in": [symbol["symbol"]] },
                        "date": { "$gte": "2017-01-01" } }
            results = self.db.iex_charts.find( query ).sort("date", DESCENDING)
            chart = pandas.DataFrame()
            for doc in results:
                chart = chart.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
            duplicates = chart[chart.duplicated(['date'])]
            #print( duplicates )
        
            # Remove all duplicates in one go    
            #if not duplicates.empty:
            #    self.db.iex_charts.delete_many({"_id":{"$in":duplicates['_id'].tolist()}})
            # Remove duplicates if they exist
            if not duplicates.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Deleting duplicates for " + symbol["symbol"] + "      ", length = 50)
                self.db.iex_charts.delete_many({"_id":{"$in":duplicates['_id'].tolist()}})
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No duplicates for " + symbol["symbol"] + "      ", length = 50)
    
    def delete_duplicate_balancesheets(self, ref_date = "1990-01-01", when = "on"):
        """
        Delete duplicates prices in MongoDB
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, latest (Str)
        """
    
        mdb_query = Query()
        mdb_symbols = mdb_query.get_symbols()
        #mdb_symbols = mdb_symbols.iloc[ 999: , : ]
        mdb_symbols.reset_index(drop=True, inplace=True)
        #mdb_symbols = ["A"]
        #print( mdb_symbols )
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        for index, symbol in mdb_symbols.iterrows():
            #if index > 10:
            #    break
            #print( symbol )
            query = { "symbol": { "$in": [symbol["symbol"]] },
                        "reportDate": { "$gte": ref_date } }
            results = self.db.iex_balancesheets.find( query ).sort("reportDate", DESCENDING)
            balancesheets = pandas.DataFrame()
            for doc in results:
                balancesheets = balancesheets.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
            duplicates = balancesheets[balancesheets.duplicated(['reportDate'])]
            print( duplicates )
        
            # Remove all duplicates in one go    
            #if not duplicates.empty:
            #    self.db.iex_charts.delete_many({"_id":{"$in":duplicates['_id'].tolist()}})
            # Remove duplicates if they exist
            if not duplicates.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Deleting duplicates for " + symbol["symbol"] + "      ", length = 50)
                #self.db.iex_balancesheets.delete_many({"_id":{"$in":duplicates['_id'].tolist()}})
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No duplicates for " + symbol["symbol"] + "      ", length = 50)
    
    def delete_dividends(self, ref_date = "1990-01-01", when = "after"):
        """
        Delete dividends from transactions
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, latest (Str)
        """
    
        query = { "type": "dividend",
                  "date": { "$gte": ref_date } }
        results = self.db.pf_transactions.find( query )
        transactions = pandas.DataFrame()
        for doc in results:
            transactions = transactions.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
        if not transactions.empty:
            self.db.pf_transactions.delete_many({"_id":{"$in":transactions['_id'].tolist()}})
    
    def delete_performance(self, ref_date = "1990-01-01", when = "after"):
        """
        Delete incorrect performances from MongoDB
        @params:
            ref_date    - Optional  : date YYYY-MM-DD (Str)
            when        - Optional  : on, latest (Str)
        """
    
        query = { "date": { "$gte": ref_date } }
        results = self.db.pf_performance.find( query )
        performances = pandas.DataFrame()
        for doc in results:
            performances = performances.append( pandas.DataFrame.from_dict(doc, orient='index').T, ignore_index=True, sort=False )
        if not performances.empty:
            self.db.pf_performance.delete_many({"_id":{"$in":performances['_id'].tolist()}})
    
    #Delete prices before 2018 from MongoDB because it was full
    def delete_prices(self):
        query = { "date": { "$lt": "2018-06-20" } }
        self.db.iex_charts.delete_many( query )
