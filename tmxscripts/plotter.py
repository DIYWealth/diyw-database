#!/usr/bin/env python
# Author: John Walker
# Date: January 7th, 2019
# Brief: A script that uses the 'plotter_tools' module to 
#        query the diywealth database and plot stock info
# Usage: ./plotter "AW.UN"
# Would plot stock information for "AW.UN"

import plotter_tools
import os
import sys
import string
import datetime
import argparse

# Check the date format is valid
def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == '__main__':

        # Parse arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', "--symbol", dest="symbol", help='Stock symbol e.g. "AW.UN"', required=True)
        parser.add_argument('-b', "--startdate", dest="startdate", help="The Start Date - format YYYY-MM-DD ", required=True, type=valid_date)
        parser.add_argument('-e', "--enddate", dest="enddate", help="The End Date - format YYYY-MM-DD (Inclusive)", required=True, type=valid_date)
        args = parser.parse_args()

        stock_symbol = str(args.symbol) # Stock symbol to request
        start_date = args.startdate # Start date
        end_date = args.enddate.replace(hour=23, minute=59, second=59, microsecond=999999) # Set end date to end of day to be inclusive

        print( "Plotting information for symbol: " + stock_symbol )
        plotter_tools.plot_open_value( stock_symbol, start_date, end_date )

        print( "Top ranked stocks:")
        top_stocks = plotter_tools.top_stocks_list( start_date )
        for i, stock in enumerate(top_stocks[:10]):
                print( stock + ": " + str(i) )

        print( "Plotting portfolio valuation: " )
        start_value = [100.0] * 10
        plotter_tools.plot_portfolio_value( top_stocks[:10], start_value, start_date, end_date )
