import pymongo
import logging 
from Decorators import logging_flow,display_index
from Exceptions import NotExistedError

logger = logging.getLogger(__name__)


class DataBaseServerConnection:
  """
  connecting to the mongodb server
  """
  def __init__(self, mongo_uri):
    """
    mongo_uri in the format 'mongodb://localhost:27017'
    """
    self.mongo_uri = mongo_uri
    self.client = None

  def __enter__(self):
      self.client = pymongo.MongoClient(self.mongo_uri)
      return self.client 

  def __exit__(self, exc_type, exc_value, exc_traceback):
    """ not return true, if error occurs then it will throw an exception"""
    self.client.close()

  def __repr__(self):
    return f'DataBaseServerConnection( port: {self.mongo_uri})'

class DataBaseManager:
  """
  connecting to database and collection 
  containing methods to list all collections, all dababases and all indexes of a collection 
  """
  def __init__(self, db_server, database_name, collection_name):
    """db_server is a DataBaseServerConnection instance """
    self.db_server = db_server
    self.database_name = database_name
    self.collection_name = collection_name
    self.database = None
    self.collection = None

  def __repr__(self):
    return f'DataBaseManager( database: {self.database_name},collection: {self.collection_name})'
    
  def connect_db(self):
    if self.database_name not in self.get_all_dbs():
      raise NotExistedError
    else:
      self.database = self.db_server[self.database_name]
      return self 

  def connect_collection(self):
    if self.collection_name not in self.get_all_collections():
      raise NotExistedError
    else:
      self.collection = self.database[self.collection_name]
      return  self
  
  def get_all_collections(self):
    return self.db_server[self.database_name].list_collection_names()

  def get_all_dbs(self):
    return self.db_server.list_database_names()

  def get_all_index(self):
    """
    :return: all existing indexes on current collection
    """
    return [index['name'] for index in self.collection.list_indexes()]


class DataBaseOperation:
  """
  Calling self-defined operations  
  """
  def __init__(self,databasemanager,func,condition):
    """
    :param: databasemanager is an instance of DataBaseManager, func is an self-defined function for db operations, 
      condition is a str, it changes depends on the called func
      for instanceL, if "process_item_find" is called, condition is in the form of "{'key_1':'value_1','key_2':'value_2'(optinal)}" as the searching criteria ;
      if "create_single_index" is called, condition is "index_name"
    """
    self.databasemanager = databasemanager
    self.func = func
    self.condition = condition
    self.result = self.process()

  def process(self):
    """
    call self-defined db operation functions
    :return result from the functions (None, or documents), the function name
    """
    return self.func(self)

  def __repr__(self):
    return f'DataBaseOperation({self.func.__name__})'

  
@logging_flow
def process_item_find(databaseoperation): 
  """
  :param: databaseoperation is an instance of DataBaseOperation
  :return: a cursor instance, which allows user to iterater over all matching documents
  """  
  return databaseoperation.databasemanager.collection.find(databaseoperation.condition)
  


@logging_flow
def process_item_insert(databaseoperation):
  """
  add a document into the collection 
  """
  databaseoperation.databasemanager.collection.insert_one(databaseoperation.condition)

@logging_flow
def process_item_delete(databaseoperation):
  """
  databaseoperation.condition is in the form of {'key_1':'value_1','key_2':'value_2'(optinal)} (the second pair is optional)
  the document fits for this condition will be deleted
  if no matching was found, raise a NotExistedError. Maybe there is a input typo in the searching condition 
  """
  if len(list(databaseoperation.databasemanager.collection.find(databaseoperation.condition)))<1:
    raise NotExistedError
  else:
    databaseoperation.databasemanager.collection.delete_one(databaseoperation.condition)
 

@logging_flow
@display_index
def create_single_index(databaseoperation):
  """
  create index in the background
  """
  if databaseoperation.condition not in databaseoperation.databasemanager.get_all_index():
    databaseoperation.databasemanager.collection.create_index(databaseoperation.condition,background=True, name = databaseoperation.condition +'_index') 
  else:
    logger.info('%s already exists as %s, no need to create again',databaseoperation.condition, databaseoperation.condition +'_index')

@logging_flow
@display_index
def drop_index(databaseoperation):
  """
  drop a specified index 
  """
  if databaseoperation.condition+'_index' not in databaseoperation.databasemanager.get_all_index():
    raise NotExistedError
  else:
      databaseoperation.databasemanager.collection.drop_index( databaseoperation.condition+'_index')

