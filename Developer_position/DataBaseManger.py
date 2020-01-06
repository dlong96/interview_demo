
class DataBaseServerConnection:
  def __init__(self, mongo_uri):
    """
    mongo_uri in the format 'mongodb://localhost:27017'
    """
    self.mongo_uri = mongo_uri

  def __enter__(self):
      self.client = pymongo.MongoClient(self.mongo_uri)
      return self.client 

  def __exit__(self, exc_type, exc_value, exc_traceback):
    """ no return true, if error occurs then it will throw a exception"""
      self.client.close()

  def __repr__(self):
    return f'DataBaseServerConnection( port: {self.mongo_uri})'

class DataBaseManager:

  def __init__(self, db_server, database_name, collection_name):
    self.db_server = db_server
    self.database_name = database_name
    self.collection_name = collection_name
    self.database = None
    self.collection = None

  def __repr__(self):
    return f'DataBaseManager( database: {self.database_name},collection: {self.collection_name})'
    
  def connect_db(self):
    self.database = self.db_server[self.database_name]
    return self 

  def connect_collection(self):
    self.collection = self.database[self.collection_name]
    return  self
  
  def get_all_collections(self):
    return self.database.get_collections()
  def get_all_dbs(self):
    return self.db_server.list_database_names()
  def get_all_index(self):
    """
    :return: all existing indexes on current collection
    """
    return [index['name'] for index in self.collection.list_indexes()]


class DataBaseOperation:
  def __init__(self,databasemanager,func, condition):
    """
    :param: databasemanager is an instance of DataBaseManager, func is th self-defined function for db operation, 
      condition is a str, it changes depends on the called func
      for instanceL, if "process_item_find" is called, condition is in the form of "{'key_1':'value_1','key_2':'value_2'(optinal)}" as the searching criteria 
    """
    self.databasemanager = databasemanager
    self.func = func
    self.condition = condition

  def process(self):
    """
    call self-defined db operation functions
    :return the function name:
    """
    self.func(self)
    return self.func__name__

  def __repr__(self)
    return f'DataBaseOperation({self.process()})'
  
@logging_flow
def process_item_find(databaseoperation): 
    """
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
    :return: true if deletion is successful
    """
    if len(list(databaseoperation.databasemanager.collection.find(databaseoperation.condition)))<1:
      raise NotExistedError
    elif databaseoperation.databasemanager.collection.delete_one(databaseoperation.condition).deleted_count == 1:
      pass
    else: 
        logger.error('deletion failed')
        raise Exception("deletion failed")

@logging_flow
def create_single_index(databaseoperation):
    """
    create index in the background
    """
    logger.info('before indexing:this collection has %s', str(databaseoperation.databasemanager.get_all_index()))
    if databaseoperation.databasemanager.condition not in databasemanager.get_all_index():
        databaseoperation.databasemanager.collection.create_index(databaseoperation.condition,background=True, name = databaseoperation.condition +'_index') 
        logger.info('after indexing: %s', str(databaseoperation.databasemanager.get_all_index()))
    else:
      logger.info('%s already exists as %s, no need to create again',databaseoperation.condition, databaseoperation.condition +'_index'))


def pull(MONGO_URL,DB_name,category):
  try:
    with DataBaseServer(MONGO_URL) as session:
      conn = DataBaseManager(session, DB_name, collection)
      db = conn.connect_db()
      table = db.connect_collection()
      DataBaseOperation(table,create_single_index,condition = 'prodcut_id')
      result = DataBaseOperation(table,process_item_find,condition = {"product_id": "B00TCO0ZAA"})
      for record in result:
        logging.info('matching result %s', record)
  except Exception:
        logger.error(exc_info=True)
          
  