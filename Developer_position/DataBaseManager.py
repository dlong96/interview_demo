import pymongo
import logging 
from Decorators import logging_flow
logger = logging.getLogger(__name__)

class NotExistedError(Exception):
    def __init__(self):
        Exception.__init__(self,"Not Exists") 

class DataBaseManager:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.mongo_db = db_name

    def __repr__(self):
        return f'DataBaseManager( port: {self.mongo_uri}, database: {self.mongo_db})'
    
    @logging_flow
    def db_connector(self):
        """
        connecting to the server and the database
        if connection error occurs, catch the exception
        check if the database exists or not, if it does, connecting to it and return True 
        if the given database does not exist, raise self-defined NotExistedErrror
        :return: Boolean 
        """
        logger.info('connecting to database')
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
        except Exception:
            logger.error('failed to connect to server',exc_info=True)
            raise 
        else:
            if self.mongo_db in self.client.list_database_names():
                self.db = self.client[self.mongo_db]   
            else:
                logger.error('input database  %s does not exist', self.mongo_db)
                raise NotExistedError
    

    @logging_flow
    def collection_connected(self, collection_name):
        """
        check if the given collection_name in the database, if not, rasie a self-defined NotExistedError
        if it is in the database, then connect to the collection_name
        """
        if collection_name not in self.get_collections():
            logger.error(' "%s" not in the database ', collection_name)
            raise NotExistedError 
        else:
            self.collection = self.db[collection_name]
    
    @logging_flow
    def close_db(self):
        """
        close the connection
        """
        logger.info("stopped connection")
        self.client.close()
        
    @logging_flow
    def get_collections(self):
        """
        :return: all collections in the connected database
        """
        return self.db.list_collection_names()
    
    @logging_flow
    def get_indexes(self):
        """
        :return: all existing indexes on current collection
        """
        return [index['name'] for index in self.collection.list_indexes()]
    
    @logging_flow
    def create_single_index(self,index_field ):
        """
        create index in the background
        """
        try:
            self.collection.create_index(index_field,background=True, name = index_field +'_index') 
            logger.info('after indexing: %s', str(self.get_indexes()))
        except Exception:
            logger.error('indexing on "%s" failed ',index_field,exc_info=True )

    @logging_flow
    def drop_index(self,index_field):
        """
        drop a specified index 
        """
        try:
            self.collection.drop_index(index_field +'_index')
            logger.info('after dropping "%s", %s',index_field, str(self.get_indexes()))
        except Exception:
            logger.error('Dropping index "%s" failed ',index_field,exc_info=True)

    @logging_flow
    def process_item_find(self, search_dict):
        """
        :param: passing a dict in the form of {'key_1':'value_1','key_2':'value_2'(optinal)} as the searching criteria (the second pair is optional)
        :return: a cursor instance, which allows user to iterater over all matching documents
        """  
        return self.collection.find(search_dict)

    @logging_flow
    def process_item_insert(self, item_dict):
        """
        add a document into the collection 
        :return: true if insertion is successful
        """
        try:
            self.collection.insert_one(item_dict)
        except Exception:
            logger.error('insertion failed', exc_info=True)
            return False
        return True

    @logging_flow
    def process_item_delete(self, item_dict):
        """
        delete a document in the form in the form of {'key_1':'value_1','key_2':'value_2'(optinal)} (the second pair is optional)
        :return: true if deletion is successful
        """
        if self.collection.delete_one(item_dict).deleted_count == 1:
            return True 
        else: 
            logger.error('deletion failed')
            return False 
