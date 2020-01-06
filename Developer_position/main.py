from DataBaseManager import DataBaseManager,NotExistedError
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

if __name__ == "__main__":
    yaml_path = 'utils/logging.yaml'
    logging_setup(yaml_path)
    pull_data('localhost','amazon_review','Camera')
