#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: A short script that uses the 'iex_tools' module to 
#        extract stock information from the IEX API
# Usage: python3 iex_main.py

from utils import printProgressBar
import os
import sys
import pandas
import pymongo
#from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
import datetime
import iexscripts.diyw_mdb.insert as mdb_insert

################################################
################################################

if __name__ == '__main__':

    #Flags for inserting specific data types
    #mdb_insert.insert_symbols()
    #mdb_insert.insert_company()
    #mdb_insert.delete_prices()
    #mdb_insert.delete_duplicates()
    #mdb_insert.insert_prices()
    mdb_insert.insert_quotes()
    mdb_insert.insert_dividends()
    #mdb_insert.insert_earnings()
    #mdb_insert.insert_financials()
    mdb_insert.insert_balancesheets()
    #mdb_insert.insert_stats()
    #mdb_insert.insert_portfolio()
    #mdb_insert.insert_transactions()
    #mdb_insert.sell_all()
    #mdb_insert.buy_all()
    mdb_insert.insert_holdings()
    mdb_insert.insert_performance()
    mdb_insert.insert_stock_list()
    mdb_insert.export_stock_list()
    mdb_insert.export_performance()
