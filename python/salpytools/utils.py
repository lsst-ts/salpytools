import logging

log = logging.getLogger(__name__)

def create_logger(level=logging.NOTSET,name='default'):
    ''' Simple Logger '''
    logging.basicConfig(level=level,
                        format='[%(asctime)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(name)
    return logger


def load_SALPYlib(Device):
    '''Trick to import modules dynamically as needed/depending on the Device we want'''

    # Make sure is not already loaded i.e. visible in globals
    try:
        SALPY_lib = globals()['SALPY_{}'.format(Device)]
        log.info('SALPY_{} is already in globals'.format(Device))
        return SALPY_lib
    except:
        log.info('importing SALPY_{}'.format(Device))
        exec("import SALPY_{}".format(Device))
    else:
        raise ValueError("import SALPY_{}: failed".format(Device))
    SALPY_lib = locals()['SALPY_{}'.format(Device)]
    # Update to make it visible elsewhere -- not sure if this works
    globals()['SALPY_{}'.format(Device)] = SALPY_lib
    return SALPY_lib
