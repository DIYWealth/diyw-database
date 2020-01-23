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
from mdb import Insert
from mdb import Export
from utils import Ftp

################################################
################################################

if __name__ == '__main__':

    mdb_insert = Insert()
    mdb_insert.insert_quotes()
    mdb_insert.insert_dividends()
    mdb_insert.insert_holdings()
    mdb_insert.insert_performance()
    mdb_insert.insert_stock_list()
    
    mdb_export = Export()
    mdb_export.export_stock_list()
    mdb_export.export_performance()

    print('Upload data to webserver with FTP')
    ftp = Ftp()
    path = os.path.dirname(os.path.realpath(__file__))
    path = path + '/output/json/'
    file_list = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        for f in filenames:
            if '.json' in f:
                file_list.append(f)
        break
    ftp.placeFiles(path, file_list)
    ftp.quitConnection()
