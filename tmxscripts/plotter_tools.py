#!/usr/bin/env python
# Tools to be used by the plotter.py script

###################################
###################################

import os
import sys
import string
import datetime
import operator
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymongo
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING

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

################################################
################################################

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
days = mdates.DayLocator()  # every day
yearFmt = mdates.DateFormatter('%Y')
monthFmt = mdates.DateFormatter('%m')
dayFmt = mdates.DateFormatter('%d')

################################################
################################################

def plot_open_value( stock_symbol, start_date, end_date ):

        # Define query
        query = {
                "Symbol": stock_symbol,
                "ScrapeDate": {"$gte": start_date},
                "ScrapeDate": {"$lte": end_date},
                }

        # Find results
        results = db.find(query).sort("ScrapeDate", ASCENDING)

        # Save field info to arrays
        date = []
        value = []
        for doc in results:
                print( str(doc.get("ScrapeDate")) + ": " + doc.get("Open") )
                date.append( doc.get("ScrapeDate") )
                value.append( float(doc.get("Open")) )

        # Plot the graph
        fig, ax = plt.subplots()
        ax.plot( date, value )

        # Format the ticks
        ax.xaxis.set_major_locator(months)
        ax.xaxis.set_major_formatter(monthFmt)
        ax.xaxis.set_minor_locator(days)

        # Set axis limits
        date_min = date[0]
        date_max = date[-1]
        ax.set_xlim( date_min, date_max )

        # Format the coords message box
        def price(x):
                return '$%1.2f' % x
        ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        ax.format_ydata = price
        ax.grid(True)

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        fig.autofmt_xdate()

        plt.xlabel( "Date" )
        plt.ylabel( "Open Value" )
        plt.title( stock_symbol )
        plt.show()

def top_stocks_list( date ):
        
        # Define query
        query = {
                "ScrapeDate": { "$gte": date.replace(hour=0, minute=0, second=0, microsecond=0) ,
                                "$lte": date.replace(hour=23, minute=59, second=59, microsecond=999999) }
                }

        # Find results
        results = db.find(query)

        # Array to store documents in
        documents = []

        # Save field info to arrays
        for i, doc in enumerate(results):
                # Ignore non-numerical values
                if doc.get("P/ERatio") == "N/A" or doc.get("EPS") == "N/A":
                        continue
                tmp_doc = {}
                tmp_doc["Symbol"] = doc.get("Symbol")
                tmp_doc["P/ERatio"] = float(doc.get("P/ERatio"))
                tmp_doc["EPS"] = float(doc.get("EPS"))
                tmp_doc["Rank"] = i
                documents.append( tmp_doc )

        # Sort by P/ERatio
        documents = sorted( documents, key=operator.itemgetter("P/ERatio"), reverse=False )

        # Save rank
        for i, doc in enumerate( documents ):
                doc["Rank"] = i

        # Sort by EPS
        documents = sorted( documents, key=operator.itemgetter("EPS"), reverse=True )

        # Add EPS order to rank
        for i, doc in enumerate( documents ):
                doc["Rank"] = doc.get("Rank") + i

        # Sort by Rank
        documents = sorted( documents, key=operator.itemgetter("Rank"), reverse=False )

        # Array for output
        output = []

        # Print top stocks
        for i, doc in enumerate(documents):
                output.append( doc.get("Symbol") )
                #print( str(doc.get("Symbol")) + ": " + str(doc.get("Rank")) )

        return output

def value_portfolio( stock_list, start_value, start_date, end_date ):

        # Define query
        query = {
                "Symbol": { "$in": stock_list },
                "ScrapeDate": { "$gte": end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                                "$lte": end_date.replace(hour=23, minute=59, second=59, microsecond=999999) }
                }

        # Find results
        end_results = db.find(query)

        # Define query
        query = {
                "Symbol": { "$in": stock_list },
                "ScrapeDate": { "$gte": start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                                "$lte": start_date.replace(hour=23, minute=59, second=59, microsecond=999999) }
                }

        # Find results
        start_results = db.find(query)

        # Calculate price changes
        price_change = [0.0] * len(start_value)

        for doc in end_results:
                index = stock_list.index(doc.get("Symbol"))
                price_change[ index ] = float(doc.get("Value"))
                #print( doc.get("Symbol") + ": " + str(price_change[ index ]) )

        for doc in start_results:
                #print( doc )
                index = stock_list.index(doc.get("Symbol"))
                #print( doc.get("Symbol") + ": " + str(price_change[ index ]) )
                #print( "New value: " + str(doc.get("Value")) )
                price_change[ index ] =  price_change[ index ] / float(doc.get("Value"))

        # Calculate new values
        end_value = start_value
        for i in range( 0, len(stock_list) ):
                end_value[i] = start_value[i] * price_change[i]

        return end_value

def plot_portfolio_value( stock_list, start_value, start_date, end_date ):

        # Get list of dates
        # Define query
        query = {
                "Symbol": stock_list[0],
                "ScrapeDate": {"$gte": start_date,
                               "$lte": end_date}
                }

        # Find results
        results = db.find(query).sort("ScrapeDate", ASCENDING)

        # Save field info to arrays
        dates = []
        for doc in results:
                dates.append( doc.get("ScrapeDate") )

        portfolio_value = []
        portfolio_value.append( sum(start_value) )

        value = start_value

        for i in range(0,len(dates)-1):
                value = value_portfolio( stock_list, value, dates[i], dates[i+1] )
                portfolio_value.append( sum(value) )

        # Plot the graph
        fig, ax = plt.subplots()
        ax.plot( dates, portfolio_value )

        # Format the ticks
        ax.xaxis.set_major_locator(months)
        ax.xaxis.set_major_formatter(monthFmt)
        ax.xaxis.set_minor_locator(days)

        # Set axis limits
        date_min = start_date
        date_max = end_date
        ax.set_xlim( date_min, date_max )

        # Format the coords message box
        def price(x):
                return '$%1.2f' % x
        ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        ax.format_ydata = price
        ax.grid(True)

        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        fig.autofmt_xdate()

        plt.xlabel( "Date" )
        plt.ylabel( "Value [CAD]" )
        plt.title( "Portfolio Value" )
        plt.show()
