import logging
from contextlib import contextmanager


@contextmanager
def alter_log_level(logger, level=logging.WARN):
    previous_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    yield
    logger.setLevel(previous_level)
