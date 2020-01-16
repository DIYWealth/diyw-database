# DIYWealth


## Description

Python framework to request stock data from the IEX API and store it in MongoDB. The DIYWealth algorithm selects a list of top stocks based on their PE ratio and return on equity (ROE). A number of virtual portfolios are constructed to purchase recommended stocks dependent on the portfolio minimum market capitalisation and number of stocks to hold. The returns (including any dividend payments) are calculated daily.

A further framework in the development phase is included to scrape stock information off the TMX website.

## Table of Contents

### 1. [Directory Layout](#directory_layout)
### 2. [Installation](#installation)

## Directory Layout <a id="directory_layout"></a>

```bash
.
+-- iex_scripts                         # Scripts to interface with IEX, MongoDB, and to do all backend processing for DIYWealth
  +-- data                              # Directory to save json files in before uploading to web server
  +-- iex                               # Library to interface with the IEX API
  +-- iex_main.py
  +-- iex_tools.py
  +-- iexplotter_tools.py
  +-- iexplotter.py
  +-- plots                             # Directory to save any performance plots to
  +-- utils                             # Library of utility functions
+-- tmx_scripts                         # Tool to scrape stock data from the TMX website
+-- README.md                           # README documentation for the repository
```

## Installation <a id="installation"></a>

### Requirements

The following Python libraries are required for the functionality of the framework :

1. [Pandas](https://pandas.pydata.org/)
2. [Pymongo](https://api.mongodb.com/python/current/)
3. [Requests](https://requests.readthedocs.io/en/master/)
4. [Arrow](https://arrow.readthedocs.io/en/latest/)
5. [socketIO-client-nexus](https://pypi.org/project/socketIO-client-nexus/)

To download the repository use :

`git clone https://github.com/DIYWealth/diyw-database.git`

Install the project in editable mode from the `diyw-database` directory :

`python3 -m pip install -e .`
