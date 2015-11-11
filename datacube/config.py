# coding=utf-8
"""
User configuration.
"""
from __future__ import absolute_import

import StringIO
import logging
import os
from ConfigParser import SafeConfigParser

# Config locations in order. Properties found in latter locations override former.
DEFAULT_CONF_PATHS = (
    '/etc/datacube.conf',
    os.environ.get('DATACUBE_CONFIG_PATH'),
    os.path.expanduser("~/.datacube.conf"),
    'datacube.conf'
)

# Default configuration options.
_DEFAULT_CONF = """
[datacube]
# Blank implies localhost
db_hostname:
db_database: datacube
"""


class UserConfig(object):
    """
    System configuration for the user.

    This is deliberately kept minimal: it's primarily for connection information and defaults for the
    current user.
    """

    def __init__(self, config):
        self._config = config

    @classmethod
    def find(cls, paths=DEFAULT_CONF_PATHS):
        """
        Find config from possible filesystem locations.
        :type paths: list[str]
        :rtype: UserConfig
        """
        config = SafeConfigParser()
        config.readfp(StringIO.StringIO(_DEFAULT_CONF))
        config.read([p for p in paths if p])
        return UserConfig(config)

    def _prop(self, key, section='datacube'):
        return self._config.get(section, key)

    @property
    def db_hostname(self):
        return self._prop('db_hostname')

    @property
    def db_database(self):
        return self._prop('db_database')


def init_logging(verbosity_level=0, log_queries=False):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.WARN)
    logging.getLogger('datacube').setLevel(logging.WARN - 10 * verbosity_level)
    if log_queries:
        logging.getLogger('sqlalchemy.engine').setLevel('INFO')
