import functools
import logging

logger = logging.getLogger(__name__)

def logging_flow(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("enter function : %s ", func.__name__)
        return func(*args, **kwargs)
    return wrapper

def display_index(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for arg in args:
            logger.info(" before indexing, all index : %s", str(arg.databasemanager.get_all_index()))
        func(*args, **kwargs)
        for arg in args:
             logger.info(" after indexing, all index : %s", str(arg.databasemanager.get_all_index()))
    return wrapper