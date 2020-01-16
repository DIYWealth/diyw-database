#!/usr/bin/env python
# Author: rpfs
# Date: June 27th, 2018
# Brief: Read in file and sort [variable] contained in stocks given by [letter] in either 'ascending' [a] or 'descending' [d] order.
# Usage: ./curfinsto_sort "all"/[letter] a/d
# Options: First Argument   - [variable]: The variable to sort
#          Second Argument  - "all": Sort through A-Z of files
#		                    - [letter]: Sort this letter only
#          Third Argument   - "a": sort by ascending order
#                           - "d": sort by descending order

import curfinsto
import os
import sys
import numpy as np

if __name__ == '__main__':

	var_opt = str(sys.argv[1])    # variable option
	letter_opt = str(sys.argv[2]) # letter option [or entire alphabet]
	sort_opt = str(sys.argv[3])   # sort option [ascending or descending]
	print( "-----------------------------" )
	print( "Variable to sort: " + var_opt )
	print( "Letter Options: " + letter_opt )
	print( "Sort Order: " + sort_opt )
	if ( sort_opt == "a" ): print( "ascending order" )
	if ( sort_opt == "d" ): print( "descending order" )
	
	stock = curfinsto # curfinsto object
	index = stock.get_variable_index( var_opt )+2
	var_list = []
	stock_list = []
	na_list = []
	if ( index < 0 ):
		exit()
	if ( letter_opt != "all" ):
		with open( letter_opt+"_stocks.txt", 'r' ) as in_file:
			lines = in_file.read().splitlines()
			print( "Number of stocks to sort: " +str(len(lines)) )
			for l in range( 0, len( lines ) ):
				cur_var = (((lines[ l ].split('|'))[index]).split(': '))[1]
				cur_stock = ((lines[ l ].split('|'))[0])
				cur_value = -99999.0
				try:
					cur_value = float( cur_var )
				except:
					na_list.append( cur_stock )
					continue

				cur_value = float( cur_var )
				var_list = np.append( var_list, cur_value )
				stock_list = np.append( stock_list, cur_stock ) 
			
			# Now need to sort the lists, numpy.argsort naturally 
			# lists in ascending order, so in order to get descending order
			# need to negate the array
			print( "Highest P/E stocks" )
			print( "No. of Non-N/A value stocks: " + str(len(var_list)) )
			if ( sort_opt == "a" ): args = np.argsort(var_list)
			if ( sort_opt == "d" ): args = np.argsort(-var_list)
			for s in range( 0, len( var_list ) ):
				print( "Stock: " + stock_list[ args[ s ] ]
					   +
					   ", " + var_opt
					   +
					   ": "
					   + str(var_list[ args[ s ] ]) )
		in_file.close()
		print( "-----------------------------" ) 
	else: 
		exit()			






