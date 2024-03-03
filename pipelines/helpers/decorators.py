import logging 

def count_query_logging(function):
    "A function wrapped with this decorator must return a count of affected objects"
    def wrapper(*args, **kwargs):
        logging.info(f"Ingesting with: {function.__name__}")
        count = function(*args, **kwargs)
        logging.info(f"Created or merged: {count}")
        return count
    return wrapper