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
from mdb import PortfolioManagement

################################################
################################################

if __name__ == '__main__':

    pf_mgmt = PortfolioManagement()
    pf_mgmt.insert_portfolio()
    pf_mgmt.insert_transactions()
    transactionDate = "2019-12-27"
    pf_mgmt.pf_sell_all( transactionDate )
    transactionDate = "2019-12-30"
    pf_mgmt.pf_buy_all( transactionDate )
