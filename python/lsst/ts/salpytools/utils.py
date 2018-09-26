from importlib import import_module
import logging

__all__ = ['create_logger', 'load_SALPYlib']


log = logging.getLogger(__name__)


def create_logger(name='default'):
    """Create a simple logger.

    Parameters
    ----------
    name: str, opt
        Name of the logger. Default 'default'

    Returns
    -------
    logger
    """
    logger = logging.getLogger(name)
    return logger


def load_SALPYlib(device):
    """Import a SALPY_{device} library.

    Parameters
    ----------
    device: str
        Name of the SALPY component (e.g. scheduler for SALPY_scheduler)

    Returns
    -------
    SALPY_lib
    """
    SALPY_lib = import_module('SALPY_{}'.format(device))
    return SALPY_lib
