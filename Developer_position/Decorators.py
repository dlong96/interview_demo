import functools

def logging_flow(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("enter function : ", func.__name__)
        func(*args, **kwargs)
    return wrapper