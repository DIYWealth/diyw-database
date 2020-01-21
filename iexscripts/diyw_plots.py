#!/usr/bin/env python
# Author: John Walker
# Date: March 10th, 2019
# Brief: A script that uses the 'iexplotter_tools' module to 
#        query the diywealth database and plot portfolio performance
# Usage: python3 iexplotter.py

from mdb import Query
from utils import plot as iexplotter_tools
import os
import sys
import string
import datetime
import operator
import argparse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas

# Check the date format is valid
def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# Main function
if __name__ == '__main__':

    # Parse arguments
    #parser = argparse.ArgumentParser()
    #parser.add_argument('-s', "--symbol", dest="symbol", help='Stock symbol e.g. "AW.UN"', required=True)
    #parser.add_argument('-b', "--startdate", dest="startdate", help="The Start Date - format YYYY-MM-DD ", required=True, type=valid_date)
    #parser.add_argument('-e', "--enddate", dest="enddate", help="The End Date - format YYYY-MM-DD (Inclusive)", required=True, type=valid_date)
    #args = parser.parse_args()

    #Start date
    startDate = "2018-07-02"

    #Get list of portfolios
    portfolios = Query().mdb_get_portfolios(startDate)["portfolioID"].tolist()

    #Plot portfolio returns
    iexplotter_tools.plot_portfolio_return(portfolios, startDate)

    #Print portfolio holdings
    iexplotter_tools.print_portfolio_holdings(portfolios, startDate)

