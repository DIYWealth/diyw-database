#!/usr/bin/env python
# TMX Scraper for stock symbols and their rate of return.
# Inspired by following webpage:
# #https://stackoverflow.com/questions/8049520/web-scraping-javascript-page-with-python
#https://stackoverflow.com/questions/10309550/python-beautifulsoup-iterate-over-table
#https://stackoverflow.com/questions/37926684/entering-value-into-search-bar-and-downloading-output-from-webpage
###################################
###################################
# import dryscrape
# from bs4 import BeautifulSoup
# session = dryscrape.Session()
# session.visit(my_url)
# response = session.body()
# soup = BeautifulSoup(response)
# soup.find(id="intro-text")
## Result:
# <p id="intro-text">Yay! Supports javascript</p>
###################################
###################################

import dryscrape
from bs4 import BeautifulSoup
import os
import sys
import json
import requests
import string
import datetime
import glob
#import shutil

def get_post_data(html_soup, query):
    btn_search = 'Search'
    return { 'SearchKeyword': query,
            'btn-search.orangeButton': btn_search }
	
################################################
################################################

if __name__ == '__main__':

	# example: ./tmx_scrap "A" "A_symbols"

	in_macro   = str(sys.argv[0])   # Input File
	letter     = str(sys.argv[1])   # Letter to scrape
	out_file   = str(sys.argv[2])   # Output file
	
	#base_url = "https://www.tmxmoney.com/en/research/listed_company_directory.html#"
	#cur_url = base_url + str(letter);
	base_url = "https://www.tsx.com/json/company-directory/search/tsx/"
	cur_url = base_url +str(letter)+ "?"
	print( "Visiting: " + cur_url )

	with open( out_file, 'w' ) as out_f:
	
		session = dryscrape.Session()
		session.visit( str(cur_url) )
		#response = session.body()
		#print( response.json() )
		#soup = BeautifulSoup( response, "lxml" )
		#json_object = json.load(soup)
		#payload = get_post_data( soup, "^A" )
		response = requests.get(str(cur_url))
		data = response.text
		d = json.loads(str(data))
		for sym in range( len(d['results']) ):
			print( "Name:"+str(d['results'][sym]['name']) )
			print( "Symbol:"+str(d['results'][sym]['symbol']) )
			print( "-----------------------" )
		print d['results'][0]
		print ( str(len(d['results'])) )
		#print( data )
		#json_d = json.dumps(data)
		#print( "#################" )
		#print( json_d )
		json_l = json.loads(data)
		#for ip in data.iteritems():
		#	print( ip['symbol'] ) 
		#	print( ip['name'] )
		#print( response )
		#print( r )
		#print ( "r Above" )
		#print( soup )
		#json_data = json.loads(response.text)
		#array = str(soup).split( "[{" )
		#for sym in range( 0, len(array) ):
		#	print (array[sym])
		#table1 = soup.find( id="tresults" )
		#print ( table1 )
		#table2 = table1.find( "tbody" )
		#print( table2 )
		#table3 = table2.find_all( "tr" )
		#print ( table3 )
		#for row in table3:
		#	for biglink in row.find_all( 'td' ):
		#		for link in biglink.find_all( 'a' ):
		#			print ( link.get( 'href' ) )
			#for biglink in row.find_all( 'a' ):
			#	print( biglink.get('href') )
			#print t
		#table.find_all( lambda tag: tag.name=='tr' )
		#for link in soup.find_all( 'a' ):
		#for link in soup.find( class="fullTableWrapper" ).find_all( 'a' ):
		#	print(link.get('href'))
		#A&amp;W Revenue Rylty Un
		#<a href="//web.tmxmoney.com/company.php?qm_symbol=AW.UN&amp;locale=EN">A&amp;W Revenue Rylty Un</a>

		
	out_f.close()
