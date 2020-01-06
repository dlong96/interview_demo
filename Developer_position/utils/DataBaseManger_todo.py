"""
To do: 
using content manger to keep error handling cleaner
Question:
where to put collection connection?
"""

from pymongo import MongoClient

class DataBaseManager:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.mongo_db = db_name

    def __repr__(self):
        return f'DataBaseManager( port: {self.mongo_uri}, database: {self.mongo_db})'

    def __enter__(self):
        self.client = pymongo.MongoClient(self.mongo_uri)
        return self.client 

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()