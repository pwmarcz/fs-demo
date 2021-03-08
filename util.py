import logging
import random
import time
from functools import wraps


def random_sleep():
    time.sleep(random.randrange(0, 3) * 0.5)


def trace(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        name = self.__class__.__name__ + '.' + method.__name__
        args_str = ', '.join(repr(arg) for arg in args)
        if kwargs:
            args_str += ', '
            args_str += ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
        logging.info(f'--- {name}({args_str})')
        result = method(self, *args, **kwargs)
        logging.info(f'--- {name}({args_str}) = {result!r}')
        return result

    return wrapped


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='[%(threadName)s] %(message)s')
