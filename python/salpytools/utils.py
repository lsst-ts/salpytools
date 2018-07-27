from importlib import import_module
import logging

log = logging.getLogger(__name__)

def create_logger(name='default'):
    ''' Simple Logger '''
    # logging.basicConfig(level=level,
    #                     format='[%(asctime)s] [%(levelname)s] %(message)s',
    #                     datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(name)
    return logger


def load_SALPYlib(device):

    SALPY_lib = import_module('SALPY_{}'.format(device))
    return SALPY_lib
