from DataBaseManager import DataBaseServerConnection,DataBaseManager,DataBaseOperation,create_single_index,process_item_find,drop_index
from Exceptions import NotExistedError
import logging
import yaml
import logging.config
import os 



def logging_setup(default_path = 'logging.ymal',default_level = logging.INFO):
    """
    Setup logging configuration
    """

    path = default_path
    if os.path.exists(path):
        print(f'found {path} ')
        with open(path,'rt') as f:
            config = yaml.safe_load(f)
            logging.config.dictConfig(config)
    else:
        print(f'{path} didnt exist')
        logging.basicConfig(level=default_level)

def pull_data(MONGO_URL,DB_name,category):
  """
  connect to MONGO_URL and given db, collection
  create index on 'product_id' and search for {"product_id": "B00TCO0ZAA"} print out matching result to infolog
  """
  try:
    with DataBaseServerConnection(MONGO_URL) as session:
      conn = DataBaseManager(session, DB_name, category)
      db = conn.connect_db()
      table = db.connect_collection()
      DataBaseOperation(table,create_single_index,condition = 'prodcut_id')
      product_search = DataBaseOperation(table,process_item_find,condition = {"product_id": "B00TCO0ZAA"})
      if product_search.result is not None:
        for record in product_search.result:
          logging.info('matching result %s', record)
  except Exception as e:
    logging.error('%s : ',e.args,exc_info=True)

if __name__ == "__main__":
    yaml_path = 'utils/logging.yaml'
    logging_setup(yaml_path)
    pull_data('localhost','amazon_review','Camera')
