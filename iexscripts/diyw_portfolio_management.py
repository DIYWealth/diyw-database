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
import iexscripts.diyw_mdb.portfolio_management as pf_mgmt

################################################
################################################

if __name__ == '__main__':

    pf_mgmt.insert_portfolio()
    pf_mgmt.insert_transactions()
    pf_mgmtmdb_insert.sell_all()
    pf_mgmt.buy_all()
