#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: Toolkit to access the IEX API

import json
import requests
import string
import datetime
import numpy
import pandas
from iex import reference
from iex import Stock
from iexscripts.constants import IEX_TOKEN

class Iex:

    def __init__(self):
        pass

    # reorder columns
    def set_column_sequence(self, dataframe, seq, front=True):
        """
        Takes a dataframe and a subsequence of its columns,
        returns dataframe with seq as first columns if "front" is True,
        and seq as last columns if "front" is False.
        @params:
            dataframe   - Required  : Pandas dataframe to reorder (Pandas.DataFrame)
            seq         - Required  : New order ([Int])
            front       - Optional  : Front or back (Bool)
        """
        cols = seq[:] # copy so we don't mutate seq
        for x in dataframe.columns:
            if x not in cols:
                if front: #we want "seq" to be in the front
                    #so append current column to the end of the list
                    cols.append(x)
                else:
                    #we want "seq" to be last, so insert this
                    #column in the front of the new column list
                    #"cols" we are building:
                    cols.insert(0, x)
        return dataframe[cols]
        
    def get_symbols(self, ref_symbol=None, ref_type=None):
        """
        Get symbols from IEX
        @params:
            ref_symbol  - Optional  : matching symbols (Str)
            ref_type    - Optional  : matching type (Str)
        """
    
        reference.output_format = 'dataframe'
        symbols = reference.symbols(token=IEX_TOKEN)
        
        #Select only matching symbols
        if ref_symbol is not None:
            symbols = symbols[symbols.symbol == ref_symbol]
        #Select only matching types
        if ref_type is not None:
            symbols = symbols[symbols.type == ref_type]
        
        return symbols
    
    def get_company(self, ref_symbol):
        """
        Get company information from IEX
        @params:
            ref_symbol  - Required  : symbols ([Str])
        """
    
        stock = Stock( ref_symbol )
        company = stock.company_table(token=IEX_TOKEN)
        #Remove unnecesary data
        company.drop(["website","CEO","primarySicCode","employees","address","address2","state","city","zip","country","phone"], axis=1, errors='ignore', inplace=True)
        #Reorder dataframe
        if not company.empty:
            company = self.set_column_sequence(company, ["symbol","companyName","exchange","industry","description","securityName","issueType","sector"])
            #print( company )
    
        return company
    
    def get_chart(self, ref_symbol, ref_range='1m'):
        """
        Get charts from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
            ref_range   - Optional  : date range (Str)
        """
    
        stock = Stock( ref_symbol )
        chart = stock.chart_table(ref_range, chartByDay=True, chartCloseOnly=True, token=IEX_TOKEN)
        print( chart )
        #Remove unnecesary data
        chart.drop(["volume","change","changePercent","changeOverTime","high","label","low","open","uClose","uHigh","uLow","uOpen","uVolume"], axis=1, errors='ignore', inplace=True)
        #Add symbol name column
        if not chart.empty:
            chart_len = len( chart.index )
            chart_arr = [ref_symbol] * chart_len
            chart.insert(loc=0, column='symbol', value=chart_arr)
            #Reorder dataframe
            chart = self.set_column_sequence(chart, ["symbol","date"])
    
        return chart
    
    def get_quote(self, ref_symbol):
        """
        Get quote from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
        """
    
        stock = Stock( ref_symbol )
        quote = stock.quote_table(token=IEX_TOKEN)
        #Remove unnecesary data
        quote = quote.loc[:, quote.columns.isin(["symbol","close","closeTime","marketCap","peRatio"])]
    
        if not quote.empty:
    
            #Don't return if any are null
            #Actually still need stocks with null mcap and pe for performance
            #if (quote['symbol'] != 'SPY').any() and quote.isnull().values.any():
            #    return pandas.DataFrame()
    
            if quote[['close','closeTime']].isnull().values.any():
                return pandas.DataFrame()
            
            #Change date format
            date = pandas.Timestamp(quote["closeTime"].iloc[0], unit='ms').strftime('%Y-%m-%d')
            quote.drop(["closeTime"], axis=1, errors='ignore', inplace=True)
            quote["date"] = date
            #Reorder dataframe
            quote = self.set_column_sequence(quote, ["symbol","date","close","marketCap","peRatio"])
    
        return quote
    
    def get_dividends(self, ref_symbol, ref_range='1m'):
        """
        Get dividends from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
            ref_range   - Optional  : date range (Str)
        """
    
        stock = Stock( ref_symbol )
        dividends = stock.dividends_table(ref_range, token=IEX_TOKEN)
        #print( ref_symbol )
        #print( dividends )
        #Remove unnecesary data
        dividends.drop(["recordDate","declaredDate","flag"], axis=1, errors='ignore', inplace=True)
        #Add symbol name column
        if not dividends.empty:
            dividends_len = len( dividends.index )
            dividends_arr = [ref_symbol] * dividends_len
            dividends.insert(loc=0, column='symbol', value=dividends_arr)
            #Reorder dataframe
            dividends = self.set_column_sequence(dividends, ["symbol","exDate","paymentDate","amount"])
    
        return dividends
    
    def get_earnings(self, ref_symbol):
        """
        Get earnings from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
        """
    
        stock = Stock( ref_symbol )
        earnings = stock.earnings_table(last="1", period="annual", token=IEX_TOKEN)
        #Remove unnecesary data
        earnings.drop(["consensusEPS","estimatedEPS","numberOfEstimates","EPSSurpriseDollar","yearAgoChangePercent","estimatedChangePercent","symbolId"], axis=1, errors='ignore', inplace=True)
        #Add symbol name
        if not earnings.empty:
            earnings_len = len( earnings.index )
            earnings_arr = [ref_symbol] * earnings_len
            earnings.insert(loc=0, column='symbol', value=earnings_arr)
            #Reorder dataframe
            earnings = self.set_column_sequence(earnings, ["symbol","actualEPS","announceTime","EPSReportDate","fiscalPeriod","fiscalEndDate"])
    
        return earnings
    
    def get_financials(self, ref_symbol):
        """
        Get financials from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
        """
    
        stock = Stock( ref_symbol )
        financials = stock.financials_table(period="annual", token=IEX_TOKEN)
        #Add symbol name
        if not financials.empty:
            financials_len = len( financials.index )
            financials_arr = [ref_symbol] * financials_len
            financials.insert(loc=0, column='symbol', value=financials_arr)
            #Reorder dataframe
            financials = self.set_column_sequence(financials, ["symbol","reportDate"])
    
        return financials
    
    def get_balancesheets(self, ref_symbol):
        """
        Get financials from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
        """
    
        stock = Stock( ref_symbol )
        balancesheet = stock.balancesheet_table(last=1, period="quarter", token=IEX_TOKEN)
        #Remove unnecesary data
        balancesheet = balancesheet.loc[:, balancesheet.columns.isin(["reportDate","shareholderEquity"])]
        #Add symbol name
        if not balancesheet.empty:
            balancesheet_len = len( balancesheet.index )
            balancesheet_arr = [ref_symbol] * balancesheet_len
            balancesheet.insert(loc=0, column='symbol', value=balancesheet_arr)
            #Reorder dataframe
            balancesheet = self.set_column_sequence(balancesheet, ["symbol","reportDate"])
    
        return balancesheet
    
    def get_stats(self, ref_symbol):
        """
        Get stats from IEX
        @params:
            ref_symbol  - Required  : symbol (Str)
        """
    
        stock = Stock( ref_symbol )
        stats = stock.stats_table(token=IEX_TOKEN)
        #Remove unnecesary data
        stats = stats.loc[:, stats.columns.isin(["sharesOutstanding"])]
        #Add symbol and date
        if not stats.empty:
            stats_len = len( stats.index )
            stats_arr = [ref_symbol] * stats_len
            stats.insert(loc=0, column='symbol', value=stats_arr)
            #Get current date
            currDate = datetime.datetime.now().strftime("%Y-%m-%d")
            stats_arr = [currDate] * stats_len
            stats.insert(loc=0, column='date', value=stats_arr)
            #Reorder dataframe
            stats = self.set_column_sequence(stats, ["symbol","date","sharesOutstanding"])
    
        return stats
