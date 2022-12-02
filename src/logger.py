"""Easy universal log configuration """
import logging.config
import sys
from logging import Logger

from src.environment import LOG_LEVEL


def set_log(name: str) -> Logger:
    """Removes redundancy when setting log in each file"""

    log = logging.getLogger(name)
    try:
        log.setLevel(level=LOG_LEVEL)
    except ValueError:
        logging.error(f"Invalid log level: {LOG_LEVEL}")
        sys.exit(1)

    return log
