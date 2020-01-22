#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: A short script that uses the 'iex_tools' module to 
#        extract stock information from the IEX API
# Usage: python3 iex_main.py

import os
import sys
import pandas
import pymongo
#from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
import datetime
from iexscripts.iex import Iex
from iexscripts.utils import printProgressBar
from mdb.mdb import Mdb
from mdb.query import Query
from mdb.algo import Algo

class Insert(Mdb):
    def __init__(self):
        #Inherit all methods and properties from Mdb
        super().__init__()

    #If new symbol exists then upload it
    def insert_symbols(self):
        print( "Insert new symbols" )
        mdb_query = Query()
        iex = Iex()
        #Get all common stocks from IEX
        symbols = iex.get_symbols(ref_type="cs")
        #Get SPY (S&P500 exchange traded index) from IEX
        symbols_spy = iex.get_symbols(ref_symbol="SPY")
        #Reset indices (probably not necessary)
        symbols.reset_index(drop=True, inplace=True)
        symbols_spy.reset_index(drop=True, inplace=True)
        #Append SPY to stocks
        symbols = symbols.append(symbols_spy, ignore_index=True, sort=False)
        symbols.reset_index(drop=True, inplace=True)
        #Get symbols already in MongoDB
        mdb_symbols = mdb_query.get_symbols()
        #Initial call to print 0% progress
        printProgressBar(0, len(symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        #Loop through symbols
        for index, symbol in symbols.iterrows():
            #Exclude forbidden characters
            forbidden = ["#"]
            if any( x in symbol["symbol"] for x in forbidden):
                #Update progress bar
                printProgressBar(index+1, len(symbols.index), prefix = 'Progress:', suffix = "Symbol contains forbidden character: " + symbol["symbol"] + "                     ", length = 50)
                continue
            #If MongoDB empty insert symbol
            if mdb_symbols.empty:
                #Update progress bar
                printProgressBar(index+1, len(symbols.index), prefix = 'Progress:', suffix = "Inserting new symbol: " + symbol["symbol"] + "                     ", length = 50)
                self.db.iex_symbols.insert_one( symbol.to_dict() )
            else:
                #Is symbol already in MongoDB
                mask = (mdb_symbols['iexId'] == symbol['iexId']) & (mdb_symbols['isEnabled'] == symbol['isEnabled']) & (mdb_symbols['name'] == symbol['name']) & (mdb_symbols['type'] == symbol['type']) 
                #Insert if not in MongoDB
                if mdb_symbols.loc[mask].empty:
                    #Update progress bar
                    printProgressBar(index+1, len(symbols.index), prefix = 'Progress:', suffix = "Inserting new symbol: " + symbol["symbol"] + "                     ", length = 50)
                    self.db.iex_symbols.insert_one( symbol.to_dict() )
                else:
                    #Update progress bar
                    printProgressBar(index+1, len(symbols.index), prefix = 'Progress:', suffix = "Symbol " + symbol["symbol"] + " already exists                     ", length = 50)
    
    #If new company exists then upload them
    def insert_company(self):
        print( "Insert new company information" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols = mdb_query.get_symbols()
        #Get companies already in MongoDB
        mdb_companies = mdb_query.get_company( mdb_symbols['symbol'].tolist() )
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        #Loop through symbols
        for index, mdb_symbol in mdb_symbols.iterrows():
            #Insert if no mdb company data exists
            if mdb_companies.empty:
                #Get company data from IEX
                iex_company = iex.get_company( mdb_symbol["symbol"] )
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Inserting company for " + mdb_symbol["symbol"] + "      ", length = 50)
                self.db.iex_company.insert_many( iex_company.to_dict('records') )
                continue
            #Skip company if already in MongoDB
            if not mdb_companies[ mdb_companies['symbol'] == mdb_symbol['symbol'] ].empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol["symbol"] + "      ", length = 50)
                continue
            #Get company data from IEX
            iex_company = iex.get_company( mdb_symbol["symbol"] )
            #Insert if company data exists
            if not iex_company.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Inserting company for " + mdb_symbol["symbol"] + "      ", length = 50)
                self.db.iex_company.insert_many( iex_company.to_dict('records') )
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No data for " + mdb_symbol["symbol"] + "      ", length = 50)
    
    #If new prices exist then upload them
    def insert_prices(self):
        print( "Insert new charts" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols_full = mdb_query.get_active_companies()
        mdb_symbols_full = mdb_symbols_full[mdb_symbols_full == 'SPY']
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = '', length = 50)
        idx_min = 0
        query_num = 100
        #flag = False
        while idx_min < len(mdb_symbols_full.index):
            #if idx_min > 1:
            #    break
            idx_max = idx_min + query_num
            if idx_max > len(mdb_symbols_full.index):
                idx_max = len(mdb_symbols_full.index)
            mdb_symbols = mdb_symbols_full.iloc[ idx_min:idx_max ]
            mdb_symbols.reset_index(drop=True, inplace=True)
            #Get latest price in MongoDB for each symbol up to 50 days ago
            mdb_charts = mdb_query.get_chart( mdb_symbols.tolist(), currDate, "latest" )
            #print( mdb_charts )
            #break
            #Loop through symbols
            for index, mdb_symbol in mdb_symbols.iteritems():
                #Get matching chart in MongoDB
                mdb_chart = mdb_charts[ mdb_charts['symbol'] == mdb_symbol ]
                #Get first date to insert
                date = mdb_chart.date.iloc[0]
                date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                iex_chart = pandas.DataFrame()
                while date <= currDate:
                    #Get date in correct format for IEX query
                    iex_date = (pandas.Timestamp(date)).strftime('%Y%m%d')
                    #Get IEX chart
                    iex_chart_day = iex.get_chart( mdb_symbol, ref_range=iex_date )
                    iex_chart = iex_chart.append(iex_chart_day, ignore_index=True, sort=False)
                    #Increment date
                    date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                #Get 1y of charts from IEX
                #print( mdb_symbol )
                #print( mdb_symbol["symbol"] )
                #if mdb_symbol["symbol"] == "ZZZZZZZZZ":
                #    flag = True
                #if not flag:
                #    continue
                #iex_chart = iex.get_chart( mdb_symbol, ref_range='5d' )
                #Get matching chart in MongoDB
                #mdb_chart = mdb_charts[ mdb_charts['symbol'] == mdb_symbol ]
                #Select charts more recent than MongoDB
                #if not iex_chart.empty and not mdb_chart.empty:
                #    mask = iex_chart['date'] > mdb_chart['date'].iloc[0]
                #    iex_chart = iex_chart.loc[mask]
                #Insert if charts exist
                #print( iex_chart )
                if not iex_chart.empty:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "Inserting chart for " + mdb_symbol + "      ", length = 50)
                    #Print write error if couldn't insert charts
                    try:
                        print( iex_chart )
                        self.db.iex_charts.insert_many( iex_chart.to_dict('records') )
                    except BulkWriteError as bwe:
                        print( bwe.details )
                        raise
                else:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
            idx_min = idx_min + query_num
    
    #If new quotes exist then upload them
    def insert_quotes(self):
        print( "Insert new quotes" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols_full = mdb_query.get_active_companies()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = '', length = 50)
        idx_min = 0
        query_num = 100
        while idx_min < len(mdb_symbols_full.index):
            idx_max = idx_min + query_num
            if idx_max > len(mdb_symbols_full.index):
                idx_max = len(mdb_symbols_full.index)
            mdb_symbols = mdb_symbols_full.iloc[ idx_min:idx_max ]
            mdb_symbols.reset_index(drop=True, inplace=True)
            #Get latest price in MongoDB for each symbol up to 50 days ago
            mdb_quotes = mdb_query.get_quotes( mdb_symbols.tolist(), currDate, "latest" )
            #Loop through symbols
            for index, mdb_symbol in mdb_symbols.iteritems():
                #Get matching quote in MongoDB
                if not mdb_quotes.empty:
                    mdb_quote = mdb_quotes[ mdb_quotes['symbol'] == mdb_symbol ]
                else:
                    mdb_quote = mdb_quotes
                #continue if already up to date
                if not mdb_quote.empty and (mdb_quote['date'].iloc[0] == currDate):
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
                    continue
                #Get quote from IEX
                iex_quote = iex.get_quote( mdb_symbol )
                #Select quotes more recent than MongoDB
                if not iex_quote.empty and not mdb_quote.empty:
                    mask = iex_quote['date'] > mdb_quote['date'].iloc[0]
                    iex_quote = iex_quote.loc[mask]
                #Insert if quotes exist
                #print( iex_quote )
                if not iex_quote.empty:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "Inserting quote for " + mdb_symbol + "      ", length = 50)
                    self.db.iex_quotes.insert_many( iex_quote.to_dict('records') )
                else:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
            idx_min = idx_min + query_num
    
    #If new dividends exist then upload them
    def insert_dividends(self):
        print( "Insert new dividends" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        #mdb_symbols = mdb_query.get_active_companies()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
    
        #Get existing portfolios
        portfolios = mdb_query.get_portfolios(currDate)[["portfolioID","inceptionDate"]]
        #Loop through portfolios
        mdb_symbols = pandas.DataFrame()
        for portfolio_index, portfolio_row in portfolios.iterrows():
            #Get portfolioID and inceptionDate
            portfolio = portfolio_row['portfolioID']
            inceptionDate = portfolio_row['inceptionDate']
            #Default to calculating holdings from inception
            date = inceptionDate
            #Get current holdings table
            holdings = mdb_query.get_holdings(portfolio, inceptionDate, "after")
            #print( holdings )
            mdb_symbols = mdb_symbols.append(holdings, ignore_index=True, sort=False)
            #print( mdb_symbols )
        mdb_symbols = mdb_symbols[mdb_symbols['symbol'] != 'USD']
        mdb_symbols = mdb_symbols['symbol'].unique().tolist()
        #print( mdb_symbols )
        #quit()
    
        #Get latest dividend in MongoDB for each symbol
        mdb_dividends = mdb_query.get_dividends( mdb_symbols, currDate, "latest" )
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols), prefix = 'Progress:', suffix = '', length = 50)
        #flag = False
        #Loop through symbols
        for index, mdb_symbol in enumerate(mdb_symbols):
            #if mdb_symbol["symbol"] == "ZZZZZZZZZ":
            #    flag = True
            #if not flag:
            #    continue
            #Get 1m of dividends from IEX
            iex_dividends = iex.get_dividends( mdb_symbol, ref_range='1m' )
            #Get matching dividend in MongoDB
            mdb_dividend = mdb_dividends[ mdb_dividends['symbol'] == mdb_symbol ]
            #Select dividends more recent than MongoDB
            if not mdb_dividend.empty and not iex_dividends.empty:
                mask = iex_dividends['exDate'] > mdb_dividend['exDate'].iloc[0]
                iex_dividends = iex_dividends.loc[mask]
            #Insert if dividends exist
            if not iex_dividends.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols), prefix = 'Progress:', suffix = "Inserting dividend for " + mdb_symbol + "      ", length = 50)
                #print( iex_dividends )
                self.db.iex_dividends.insert_many( iex_dividends.to_dict('records') )
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
    
    #If new earnings exist then upload them
    def insert_earnings(self):
        print( "Insert new earnings" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols = mdb_query.get_active_companies()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Get latest earnings in MongoDB for each symbol
        mdb_earnings = mdb_query.get_earnings( mdb_symbols.tolist(), currDate, "latest" )
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        #Loop through symbols
        for index, mdb_symbol in mdb_symbols.iteritems():
            #Get earnings from IEX
            iex_earnings = iex.get_earnings( mdb_symbol )
            #Get matching earning in MongoDB
            mdb_earning = mdb_earnings[ mdb_earnings['symbol'] == mdb_symbol ]
            #Select earnings more recent than MongoDB
            if not mdb_earning.empty and not iex_earnings.empty:
                mask = iex_earnings['fiscalEndDate'] > mdb_earning['fiscalEndDate'].iloc[0]
                iex_earnings = iex_earnings.loc[mask]
            #Insert if earnings exist
            if not iex_earnings.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Inserting earnings for " + mdb_symbol + "      ", length = 50)
                self.db.iex_earnings.insert_many( iex_earnings.to_dict('records') )
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
    
    #If new financials exist then upload them
    def insert_financials(self):
        print( "Insert new financials" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols = mdb_query.get_active_companies()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Get latest financials in MongoDB for each symbol
        mdb_financials = mdb_query.get_financials( mdb_symbols.tolist(), currDate, "latest" )
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        #Loop through symbols
        for index, mdb_symbol in mdb_symbols.iteritems():
            #Get financials from IEX
            iex_financials = iex.get_financials( mdb_symbol )
            #Get matching financial in MongoDB
            mdb_financial = mdb_financials[ mdb_financials['symbol'] == mdb_symbol ]
            #Select financials more recent than MongoDB
            if not mdb_financial.empty and not iex_financials.empty:
                mask = iex_financials['reportDate'] > mdb_financial['reportDate'].iloc[0]
                iex_financials = iex_financials.loc[mask]
            #Insert if financials exist
            if not iex_financials.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Inserting financials for " + mdb_symbol + "      ", length = 50)
                self.db.iex_financials.insert_many( iex_financials.to_dict('records') )
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
    
    #If new balancesheets exist then upload them
    def insert_balancesheets(self):
        print( "Insert new balance sheets" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols = mdb_query.get_active_companies()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        threeMonthsAgo = (pandas.Timestamp(currDate) + pandas.DateOffset(days=-120)).strftime('%Y-%m-%d')
        #Get latest balancesheets in MongoDB for each symbol
        mdb_balancesheets = mdb_query.get_balancesheets( mdb_symbols.tolist(), currDate, "latest" )
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols.index), prefix = 'Progress:', suffix = '', length = 50)
        #flag = False
        #Loop through symbols
        for index, mdb_symbol in mdb_symbols.iteritems():
            #if mdb_symbol == 'YETI':
            #    flag = True
            #if not flag:
            #    continue
            #Get matching balancesheet in MongoDB
            if not mdb_balancesheets.empty:
                mdb_balancesheet = mdb_balancesheets[ mdb_balancesheets['symbol'] == mdb_symbol ]
            else:
                mdb_balancesheet = mdb_balancesheets
            #Skip is less than 3 months since most recent
            if not mdb_balancesheet.empty: 
                mask = mdb_balancesheet['reportDate'] > threeMonthsAgo
                mdb_recent_balancesheet = mdb_balancesheet.loc[mask]
                if not mdb_recent_balancesheet.empty:
                    #Update progress bar
                    printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Data too recent for " + mdb_symbol + "      ", length = 50)
                    continue
            #Get balancesheets from IEX
            iex_balancesheets = iex.get_balancesheets( mdb_symbol )
            #Select balancesheets more recent than MongoDB
            if not mdb_balancesheet.empty and not iex_balancesheets.empty:
                mdb_balancesheet = mdb_balancesheet.sort_values(by='reportDate', ascending=False, axis='index')
                mask = iex_balancesheets['reportDate'] > mdb_balancesheet['reportDate'].iloc[0]
                iex_balancesheets = iex_balancesheets.loc[mask]
            #Insert if balancesheets exist
            if not iex_balancesheets.empty:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "Inserting balancesheets for " + mdb_symbol + "      ", length = 50)
                self.db.iex_balancesheets.insert_many( iex_balancesheets.to_dict('records') )
            else:
                #Update progress bar
                printProgressBar(index+1, len(mdb_symbols.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
    
    #If new stats exist then upload them
    def insert_stats(self):
        print( "Insert new stats" )
        mdb_query = Query()
        iex = Iex()
        #Get all symbols in MongoDB
        mdb_symbols_full = mdb_query.get_active_companies()
        #mdb_symbols_full = mdb_symbols_full[mdb_symbols_full == 'A']
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Initial call to print 0% progress
        printProgressBar(0, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = '', length = 50)
        idx_min = 0
        query_num = 100
        #flag = True
        while idx_min < len(mdb_symbols_full.index):
            #if idx_min > 1:
            #    break
            idx_max = idx_min + query_num
            if idx_max > len(mdb_symbols_full.index):
                idx_max = len(mdb_symbols_full.index)
            mdb_symbols = mdb_symbols_full.iloc[ idx_min:idx_max ]
            mdb_symbols.reset_index(drop=True, inplace=True)
            #Get latest price in MongoDB for each symbol up to 50 days ago
            mdb_stats = mdb_query.get_stats( mdb_symbols.tolist(), currDate, "latest" )
            #Loop through symbols
            for index, mdb_symbol in mdb_symbols.iteritems():
                #Get stat from IEX
                #print( mdb_symbol )
                #print( mdb_symbol["symbol"] )
                #if mdb_symbol == "BAH":
                #    flag = False
                #if flag:
                #    continue
                iex_stat = iex.get_stats( mdb_symbol )
                #Get matching stat in MongoDB
                if not mdb_stats.empty:
                    mdb_stat = mdb_stats[ mdb_stats['symbol'] == mdb_symbol ]
                else:
                    mdb_stat = mdb_stats
                #Select stats more recent than MongoDB
                if not iex_stat.empty and not mdb_stat.empty:
                    mask = iex_stat['date'] > mdb_stat['date'].iloc[0]
                    iex_stat = iex_stat.loc[mask]
                #Insert if stats exist
                #print( iex_stat )
                if not iex_stat.empty:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "Inserting stat for " + mdb_symbol + "      ", length = 50)
                    self.db.iex_stats.insert_many( iex_stat.to_dict('records') )
                else:
                    #Update progress bar
                    printProgressBar(idx_min+index+1, len(mdb_symbols_full.index), prefix = 'Progress:', suffix = "No new data for " + mdb_symbol + "      ", length = 50)
            idx_min = idx_min + query_num
    
    #TODO:
    #Keep track of corporate actions
    #/ref-data/daily-list/corporate-actions
    
    #Insert holdings table and update when a new transaction is inserted
    #At the moment this won't pick up if more transactions are made on the same day as the last update
    #Can't change holdings retrospectively - new transactions must be after lastUpdated date
    def insert_holdings(self):
        print( "Calculate portfolio holdings" )
        mdb_query = Query()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #currDate = "2019-12-30"
        #Get existing portfolios
        portfolios = mdb_query.get_portfolios(currDate)[["portfolioID","inceptionDate"]]
        #Loop through portfolios
        for portfolio_index, portfolio_row in portfolios.iterrows():
            #Get portfolioID and inceptionDate
            portfolio = portfolio_row['portfolioID']
            inceptionDate = portfolio_row['inceptionDate']
            print( 'Calculating holdings for ', portfolio )
            #Default to calculating holdings from inception
            date = inceptionDate
            #Get current holdings table
            holdings = mdb_query.get_holdings(portfolio, currDate, "on")
            #print( holdings )
            #If holdings exist then calculate holdings from next date
            if not holdings.empty:
                date = holdings['lastUpdated'].max()
                date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
            #If no existing holdings create 0 dollar entry to create table
            if holdings.empty:
                holding_dict = { "portfolioID": portfolio,
                                    "symbol": "USD",
                                    "endOfDayQuantity": 0.0,
                                    "lastUpdated": inceptionDate }
                holdings = pandas.DataFrame.from_dict(holding_dict, orient='index').T
            #Get all new transactions
            transactions = mdb_query.get_transactions(portfolio, date, "after")
    
            #Loop through dates and update holdings table
            while date <= currDate:
    
                #Insert dividends to transactions database and update transactions table
                #Dividends will be ahead of time so will be ready to be added on the correct date
    
                #Get dividends for all holdings except USD
                div_holdings = holdings[ ~holdings["symbol"].isin(["USD"]) ]
                dividends = mdb_query.get_dividends(div_holdings["symbol"].unique().tolist(), date, "on")
                #Get rid of any non-USD dividends
                if not dividends.empty:
                    dividends = dividends[ dividends['currency'] == 'USD' ]
    
                if not dividends.empty:
                    #Get dividends with exDate = date
                    div_date = dividends[ dividends.exDate == date ]
                    #Loop through dividends
                    for d_index, dividend in div_date.iterrows():
                        #print( dividend )
                        #Is dividend already in transactions?
                        #If not insert it
                        #Skip dividends with bad data entries
                        if not dividend.amount:
                            print( "Skipping dividend as amount is empty!" )
                            continue
                        if dividend.paymentDate == None:
                            print( "Skipping dividend as paymentDate is None!" )
                            continue
                        transactions_paymentDate = transactions
                        if not transactions.empty:
                            transactions_paymentDate = transactions[ (transactions.date == dividend.paymentDate) & (transactions.symbol == dividend.symbol) & (transactions.type == 'dividend') ]
                        holding_quantity = holdings[holdings["symbol"] == dividend.symbol]["endOfDayQuantity"]
                        holding_quantity.reset_index(drop=True, inplace=True)
                        if transactions_paymentDate.empty and (holding_quantity != 0).any():
                            transaction_table = { "portfolioID": portfolio,
                                                    "symbol": dividend.symbol,
                                                    "type": "dividend",
                                                    "date": dividend.paymentDate,
                                                    "price": float(dividend.amount),
                                                    "volume": holding_quantity.iloc[0],
                                                    "commission": 0.0 }
                            transactions = transactions.append( pandas.DataFrame.from_dict(transaction_table, orient='index').T, ignore_index=True, sort=False )
                            insert_pf_transactions = True
                            if insert_pf_transactions:
                                print( "Inserting dividend: " + date )
                                print( transaction_table )
                                self.db.pf_transactions.insert_one( transaction_table )
    
                #Now attend to transactions on date
                if transactions.empty:
                    #Increment date
                    date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                    continue
                transactions_date = transactions[transactions.date == date]
                #Loop through transactions
                for t_index, transaction in transactions_date.iterrows():
                    print( "Inserting transaction:" )
                    print( transaction )
                    #Get any existing holding for the transaction symbol
                    if transaction.type == "dividend":
                        holding = holdings[holdings.symbol == "USD"]
                    else:
                        holding = holdings[holdings.symbol == transaction.symbol]
                    holding.reset_index(drop=True, inplace=True)
                    #print( holding )
                    #Remove that holding from holdings table
                    if not holding.empty:
                        holdings = holdings[ ~holdings["symbol"].isin([holding['symbol'].iloc[0]]) ]
                    #print( holdings )
                    #Add any dividends to the holdings table
                    if transaction.type == "dividend":
                        holding["endOfDayQuantity"] = holding["endOfDayQuantity"] + (transaction.price * transaction.volume)
                        holding["lastUpdated"] = date
                        holdings = holdings.append( holding, ignore_index=True, sort=False )
                    #Add any deposits to the holdings table
                    if transaction.type == "deposit":
                        holding["endOfDayQuantity"] = holding["endOfDayQuantity"] + (transaction.price * transaction.volume)
                        holding["lastUpdated"] = date
                        holdings = holdings.append( holding, ignore_index=True, sort=False )
                    #Add any stocks purchased to the holdings table
                    if transaction.type == "buy":
                        holding_dict = {}
                        if not holding.empty:
                            holding_dict = { "portfolioID": transaction.portfolioID,
                                             "symbol": transaction.symbol,
                                             "endOfDayQuantity": holding["endOfDayQuantity"].iloc[0] + transaction.volume,
                                             "lastUpdated": date }
                        else:
                            holding_dict = { "portfolioID": transaction.portfolioID,
                                             "symbol": transaction.symbol,
                                             "endOfDayQuantity": transaction.volume,
                                             "lastUpdated": date }
                        #print( holding_dict )
                        holdings = holdings.append( pandas.DataFrame.from_dict(holding_dict, orient='index').T, ignore_index=True, sort=False )
                        #Adjust cash entry accordingly
                        cash = holdings[holdings.symbol == "USD"]
                        holdings = holdings[ ~holdings["symbol"].isin(["USD"]) ]
                        cash["endOfDayQuantity"] = cash["endOfDayQuantity"] - (transaction.price * transaction.volume)
                        cash["lastUpdated"] = date
                        holdings = holdings.append( cash, ignore_index=True, sort=False )
                    #print( holdings )
                    #Remove any stocks sold from the holdings table
                    if transaction.type == "sell":
                        if not holding.empty:
                            holding["endOfDayQuantity"] = holding["endOfDayQuantity"] - transaction.volume
                            holding["lastUpdated"] = date
                            holdings = holdings.append( holding, ignore_index=True, sort=False )
                        else:
                            raise Exception("Trying to sell unowned stock!")
                        #Adjust cash entry accordingly
                        cash = holdings[holdings.symbol == "USD"]
                        holdings = holdings[ ~holdings["symbol"].isin(["USD"]) ]
                        cash["endOfDayQuantity"] = cash["endOfDayQuantity"] + (transaction.price * transaction.volume)
                        cash["lastUpdated"] = date
                        holdings = holdings.append( cash, ignore_index=True, sort=False )
                    #print( holdings )
                #Upload new holdings entries to MongoDB
                holdings_date = holdings[holdings.lastUpdated == date]
                if not holdings_date.empty:
                    print( "New holdings:" )
                    print( holdings_date )
                    insert_holdings_tx = True
                    if insert_holdings_tx:
                        print( "Inserting holdings for " + portfolio )
                        self.db.pf_holdings.insert_many( holdings_date.to_dict('records') )
                #Increment date
                date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
    
    #Insert portfolio performance table
    #Calculate portfolio value - close of day prices for holdings
    #Calculate portfolio return - (close of day holdings - (close of previous day holding + purchases))/(close of previous day holding + purchases)
    def insert_performance(self):
        print( "Insert portfolio performance tables" )
        mdb_query = Query()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #currDate = "2019-12-27"
        #Get existing portfolios
        portfolios = mdb_query.get_portfolios(currDate)[["portfolioID","inceptionDate"]]
        #Loop through portfolios
        for portfolio_index, portfolio_row in portfolios.iterrows():
            #Get portfolioID and inceptionDate
            portfolio = portfolio_row.portfolioID
            inceptionDate = portfolio_row.inceptionDate
            print( 'Inserting performance tables for ' + portfolio )
            #Get holdings tables from inception
            holdings = mdb_query.get_holdings(portfolio, inceptionDate, "after").sort_values(by="lastUpdated", ascending=False, axis="index")
            #print( holdings )
            #Default to calculating performance from inception
            date = inceptionDate
            #Get list of symbols in holdings table
            symbols = holdings["symbol"].unique().tolist()
            #Get existing performance table for portfolio sorted by date
            performance = mdb_query.get_performance([portfolio], inceptionDate)
            if not performance.empty:
                performance.sort_values(by="date", ascending=False, axis="index", inplace=True)
            #Get close value from last date and increment the date
            perf_tables = []
            prevCloseValue = 0
            adjPrevCloseValue = 0
            if not performance.empty:
                date = performance.iloc[0]["date"]
                date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                adjPrevCloseValue = performance.iloc[0]["closeValue"]
                prevCloseValue = performance.iloc[0]["closeValue"]
            #print( date )
            #Get prices for symbols in portfolio after date
            prices = mdb_query.get_quotes(symbols, date, "after")
            #print( prices )
            #If there are no prices then can't calculate performance
            if prices.empty:
                print( "No prices!" )
                continue
            #Get any transactions after date
            transactions = mdb_query.get_transactions(portfolio, date, "after")
            #print( transactions )
            #Loop through dates
            while date <= currDate:
                #print( date )
                #Initialize portfolio close of day values
                closeValue = 0
                adjCloseValue = 0
                #Get latest holding for each symbol on date
                holdings_date = holdings[holdings.lastUpdated <= date]
                holdings_date = holdings_date[holdings_date.groupby(['symbol'], sort=False)['lastUpdated'].transform(max) == holdings_date['lastUpdated']]
                #Merge with stock prices
                holdings_date = pandas.merge(holdings_date,prices[prices.date == date],how='left',left_on=["symbol"],right_on=["symbol"],sort=False)
                #Remove stocks no longer held
                holdings_date = holdings_date[ holdings_date['endOfDayQuantity'] != 0 ]
                #print( holdings_date )
                #Skip if only USD in holdings
                if holdings_date[holdings_date.symbol != "USD"].empty:
                    date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                    continue
                #Skip any day where there aren't prices for all stocks
                if holdings_date[holdings_date.symbol != "USD"]['close'].isnull().values.any():
                    date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                    continue
                #Calculate portfolio close of day value from close of day stock prices
                if not holdings_date.empty:
                    for index, holding in holdings_date.iterrows():
                        if holding.symbol == "USD":
                            closeValue = closeValue + (holding.endOfDayQuantity)
                        else:
                            closeValue = closeValue + (holding.endOfDayQuantity * holding.close)
                #Get any deposits or withdrawals
                deposits = pandas.DataFrame()
                withdrawals = pandas.DataFrame()
                if not transactions.empty:
                    deposits = transactions[(transactions.date == date) & (transactions.type == "deposit")]
                    withdrawals = transactions[(transactions.date == date) & (transactions.type == "withdrawal")]
                #print( deposits )
                #print( withdrawals )
                #Adjust close or previous close for withdrawals/deposits
                adjPrevCloseValue = prevCloseValue
                adjCloseValue = closeValue
                if not deposits.empty:
                    for index, deposit in deposits.iterrows():
                        adjPrevCloseValue = adjPrevCloseValue + (deposit.volume * deposit.price)
                if not withdrawals.empty:
                    for index, withdrawal in withdrawals.iterrows():
                        adjCloseValue = adjCloseValue + (withdrawal.volume * withdrawal.price)
                #If portfolio has no holdings or deposits yet then continue
                if adjPrevCloseValue == 0:
                    date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
                    continue
                #Build portfolio performance table
                perf_table = { "portfolioID": portfolio,
                                "date": date,
                                "prevCloseValue": prevCloseValue,
                                "closeValue": closeValue,
                                "adjPrevCloseValue": adjPrevCloseValue,
                                "adjCloseValue": adjCloseValue,
                                "percentReturn": 100.*((adjCloseValue-adjPrevCloseValue)/adjPrevCloseValue) }
                perf_tables.append( perf_table )
                #Reset previous close values
                prevCloseValue = closeValue
                adjPrevCloseValue = closeValue
                #Increment date
                date = (pandas.Timestamp(date) + pandas.DateOffset(days=1)).strftime('%Y-%m-%d')
            #Insert performance table
            insert_pf_performance = True
            #print( perf_tables )
            if insert_pf_performance and len(perf_tables)>0:
                #print( perf_tables )
                self.db.pf_performance.insert_many( perf_tables )
    
    #Store the top ranked stocks for the last week
    def insert_stock_list(self):
        print( "Insert ranked stock list" )
        mdb_query = Query()
        #Get current date
        currDate = datetime.datetime.now().strftime("%Y-%m-%d")
        #Delete stock lists older than one week
        #No reason to keep them
        weekBeforeDate = (pandas.Timestamp(currDate) + pandas.DateOffset(days=-7)).strftime('%Y-%m-%d')
        query = { "date": { "$lt": weekBeforeDate } }
        self.db.pf_stock_list.delete_many( query )
        #Get ranked stock list for current date
        mdb_algo = Algo()
        merged = mdb_algo.calculate_top_stocks(currDate) 
        #Skip if latest date already in database
        latestDate = merged.sort_values(by="date", ascending=False, axis="index")
        latestDate = latestDate.iloc[0]["date"]
        latestStockList = mdb_query.get_stock_list(latestDate, "on")
        if latestStockList.empty and not merged.empty:
            print( "Inserting stock list" )
            self.db.stock_list.insert_many( merged.to_dict('records') )
