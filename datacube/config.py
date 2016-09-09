# coding=utf-8
"""
User configuration.
"""
from __future__ import absolute_import

import os

from . import compat

#: Config locations in order. Properties found in latter locations override
#: earlier ones.
#:
#: - `/etc/datacube.conf`
#: - file at `$DATACUBE_CONFIG_PATH` environment variable
#: - `~/.datacube.conf`
#: - `datacube.conf`
DEFAULT_CONF_PATHS = (
    '/etc/datacube.conf',
    os.environ.get('DATACUBE_CONFIG_PATH'),
    os.path.expanduser("~/.datacube.conf"),
    'datacube.conf'
)

# Default configuration options.
_DEFAULT_CONF = u"""
[datacube]
# Blank implies localhost
db_hostname:
db_database: datacube
# If a connection is unused for this length of time, expect it to be invalidated.
db_connection_timeout: 60

[locations]
# Where to reach storage locations from the current machine.
#  -> Location names (here 'eotiles') are arbitrary, but correspond to names used in the
#     storage types.
#  -> We may eventually support remote protocols (http, S3, etc) to lazily fetch remote data.
# Define these in your own datacube.conf file.
# eotiles: file:///g/data/...
"""


class LocalConfig(object):
    """
    System configuration for the user.

    This is deliberately kept minimal: it's primarily for connection information and defaults for the
    current user.
    """

    def __init__(self, config, files_loaded=None):
        self._config = config
        self.files_loaded = []
        if files_loaded:
            self.files_loaded = files_loaded

    @classmethod
    def find(cls, paths=DEFAULT_CONF_PATHS):
        """
        Find config from possible filesystem locations.
        :type paths: list[str]
        :rtype: LocalConfig
        """
        config = compat.read_config(_DEFAULT_CONF)
        files_loaded = config.read([p for p in paths if p])
        return LocalConfig(config, files_loaded)

    def _prop(self, key, section='datacube'):
        try:
            return self._config.get(section, key)
        except compat.NoOptionError:
            return None

    @property
    def db_hostname(self):
        return self._prop('db_hostname')

    @property
    def db_database(self):
        return self._prop('db_database')

    @property
    def db_connection_timeout(self):
        return int(self._prop('db_connection_timeout'))

    @property
    def location_mappings(self):
        """
        :rtype: dict[str, str]
        """
        return dict(self._config.items('locations'))

    @property
    def db_username(self):
        try:
            import pwd
            default_username = pwd.getpwuid(os.geteuid()).pw_name
        except ImportError:
            # No default on Windows
            default_username = None

        return self._prop('db_username') or default_username

    @property
    def db_password(self):
        return self._prop('db_password')

    @property
    def db_port(self):
        return self._prop('db_port') or '5432'

    def __str__(self):
        return "LocalConfig<loaded_from={})".format(self._files_loaded or
                                                    'defaults')

    def __repr__(self):
        return self.__str__()


OPTIONS = {'reproject_threads': 4}


#: pylint: disable=invalid-name
class set_options(object):
    """Set global state within a controlled context

    Currently, the only supported options are:
    * reproject_threads: The number of threads to use when reprojecting

    You can use ``set_options`` either as a context manager::

        with datacube.set_options(reproject_threads=16):
            ...

    Or to set global options::

        datacube.set_options(reproject_threads=16)
    """
    def __init__(self, **kwargs):
        self.old = OPTIONS.copy()
        OPTIONS.update(kwargs)

    def __enter__(self):
        return

    def __exit__(self, exc_type, value, traceback):
        OPTIONS.clear()
        OPTIONS.update(self.old)
