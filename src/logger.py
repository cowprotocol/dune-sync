"""Easy universal log configuration """
import logging.config
import sys
from logging import Logger

from src.environment import LOG_LEVEL


def set_log(name: str) -> Logger:
    """
    Instantiates and returns a logger with the name defined by the caller.
    Used by any file which wants to inherit the global log level configuration
    and assigns the desired naming convention (usually `__name__`)
    """

    log = logging.getLogger(name)
    try:
        log.setLevel(level=LOG_LEVEL)
    except ValueError:
        logging.error(f"Invalid log level: {LOG_LEVEL}")
        sys.exit(1)

    return log
