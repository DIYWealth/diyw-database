#!/usr/bin/env python
# Author: J. Walker
# Date: Feb 11th, 2019
# Brief: Toolkit to access the IEX API and data stored in MongoDB.

import pymongo
from pymongo import MongoClient
from iexscripts.constants import (MDB_USER,
                                  MDB_PASSWORD,
                                  MDB_HOST,
                                  MDB_PORT,
                                  MDB_NAMESPACE)

class Mdb:
    def __init__(self):
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

        self._db = connection.diywealth

    @property
    def db(self):
        return self._db
