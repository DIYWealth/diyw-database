#!/usr/bin/env python
# Author: rpfs
# Date: April 28th, 2018
# Brief: A short script that uses the 'curfinsto' module to 
#        extract stock symbol information from online sources
# Usage: ./curfinsto_x "A" 
# Would get all stocks beginning with letter A e.g. "AW.UN"

import curfinsto
import os
import sys
import pymongo
from pymongo import MongoClient

################################################
# MongoDB connection
################################################

connection_params = {
        'user': 'jwalker',
        'password': 'R0bandJohn',
        'host': 'ds235877.mlab.com',
        'port': 35877,
        'namespace': 'diywealth',
}

connection = MongoClient(
    'mongodb://{user}:{password}@{host}:'
    '{port}/{namespace}'.format(**connection_params)
)

db = connection.diywealth.mynewcollection

#print( str(db.find_one()) )

################################################
################################################

if __name__ == '__main__':

        stock_letter = str(sys.argv[1])   # Stock Symbol to request
        print( "Requested information for symbol with letter: " + str(stock_letter) )
        stock = curfinsto
        get_by_letter_output = stock.get_stocks_by_letter( str(stock_letter), str(stock_letter)+"_stocks.txt" )
        symbol_list = get_by_letter_output[0]
        #for sym in range (len(symbol_list)):
        #        stock_info = stock.get_stock_info( symbol_list[ sym ] )[0]
        #        print( "Stock info for " + symbol_list[ sym ] )
        #        print( stock_info )
        #        print( "-----------------------------" )

        documents = get_by_letter_output[1]

        #print( documents )

        print( "Inserting documents..." )
        db.insert_many(documents)
        print( "Documents inserted." )

        
        
        #stock_info = stock.get_stock_info( str(stock_symbol) )[0]
        #for x in range (len(stock_info)):
        #        print( stock_info[ x ] ) 
        


# Database keys:

# Name: The full name of the stock as found on tsx.com
# Symbol: The stock symbol as found on tsx.com
# ScrapeDate: Date the stock information was scraped
        
# Information about stock variables available at tmx.com:

# Volume: In the context of a single stock trading on a stock exchange, the volume is commonly reported as the number of shares that changed hands during a given day.

# Value: Stock value in CAD $

# Open: Opening stock share value of last trading day.

# High: High between open and previous close values of trading day.

# Shares Out.: Outstanding shares refer to a company's stock currently held by all its shareholders, including share blocks held by institutional investors and restricted shares owned by the company's officers and insiders. Outstanding shares are shown on a company's balance sheet under the heading "Capital Stock."

# Beta: A beta of less than 1 means that the security is theoretically less volatile than the market. A beta of greater than 1 indicates that the security's price is theoretically more volatile than the market. For example, if a stock's beta is 1.2, it's theoretically 20% more volatile than the market.

# Prev. Close: Closing stock share value of last trading day.

# Market Cap: Market capitalization (market cap) is the market value at a point in time of the shares outstanding of a publicly traded company, being equal to the share price at that point of time multiplied by the number of shares outstanding.

# VMAP: In finance, volume-weighted average price (VWAP) is the ratio of the value traded to total volume traded over a particular time horizon (usually one day). ... VWAP is often used as a trading benchmark by investors who aim to be as passive as possible in their execution.

# Dividend: A dividend is a payment made by a corporation to its shareholders, usually as a distribution of profits. When a corporation earns a profit or surplus, the corporation is able to re-invest the profit in the business (called retained earnings) and pay a proportion of the profit as a dividend to shareholders.

# Dividend Freq: Frequency of divident payout

# P/E Ratio: The price/earnings ratio (often shortened to the P/E ratio or the PER) is the ratio of a company's stock price to the company's earnings per share. The ratio is used in valuing companies

# EPS: Earnings per share (EPS) is the portion of a company's profit allocated to each outstanding share of common stock. Earnings per share serves as an indicator of a company's profitability.

# Yield: In finance, the yield on a security is the amount of cash (in percentage terms) that returns to the owners of the security, in the form of interest or dividends received from it. Normally, it does not include the price variations, distinguishing it from the total return.

# P/B Ratio: The price-to-book ratio (P/B Ratio) is a ratio used to compare a stock's market value to its book value. It is calculated by dividing the current closing price of the stock by the latest quarter's book value per share. Also known as the "price-equity ratio".




