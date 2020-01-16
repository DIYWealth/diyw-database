#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: Toolkit to access the IEX API and data stored in MongoDB.

import pymongo
from pymongo import MongoClient
from constants import (MDB_USER,
                       MDB_PASSWORD,
                       MDB_HOST,
                       MDB_PORT,
                       MDB_NAMESPACE)

def get_mongodb():
    """
    Return MongoDB database object
    """

    connection_params = {
        'user': MDB_USER,
        'password': MDB_PASSWORD,
        'host': MDB_HOST,
        'port': MDB_PORT,
        'namespace': MDB_NAMESPACE,
    }

    connection = MongoClient(
        'mongodb://{user}:{password}@{host}:'
        '{port}/{namespace}'.format(**connection_params)
    )

    db = connection.diywealth

    return db
