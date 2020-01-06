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

def pull_data(MONGO_URL,MONGO_DB,category):
    """
    connecting to the server, the database and the collection
    adding index on a attribute 
    and printing the matching documents into info_log 
    :param: port,database name, attribute that build the index on 
    """
    db = DataBaseManager(MONGO_URL,MONGO_DB)
    try:
        db.db_connector()
    except NotExistedError:
        logging.error("db not existed, probably typo")
        return False
    except Exception:
        logger.error('failed to connect to server',exc_info=True)
        return False

    try:
        db.collection_connected(category)
    except NotExistedError:
        logging.error("%s doesn't exsit in %s, probably typo",category, db.mongo_db)
        return False
    else:
        all_collections = db.get_collections()
        logging.info('Database has : %s ', str(all_collections))
        logging.info('create index')
        db.create_single_index('prodcut_id')
        for result in db.process_item_find({"product_id": "B00TCO0ZAA"}):
            logging.info('matching result %s', result)
    finally:
        db.close_db()
        return True


if __name__ == "__main__":
    yaml_path = 'utils/logging.yaml'
    logging_setup(yaml_path)
    pull_data('localhost','amazon_review','Camera')
