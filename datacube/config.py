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
[DEFAULT]
# Blank implies localhost
db_hostname:
db_database: datacube
# If a connection is unused for this length of time, expect it to be invalidated.
db_connection_timeout: 60
# Which driver to activate by default in this environment (eg. "NetCDF CF", 's3')
default_driver: NetCDF CF


[user]
# Which environment to use when none is specified explicitly.
# 'datacube' was the config section name before we had environments; it's used here to be backwards compatible.
default_environment: datacube

[datacube]
# Inherit all defaults.
"""


class LocalConfig(object):
    """
    System configuration for the user.

    This is deliberately kept minimal: it's primarily for connection information and defaults for the
    current user.
    """

    def __init__(self, config, environment=None, files_loaded=None):
        self._config = config
        self.files_loaded = []
        if files_loaded:
            self.files_loaded = files_loaded

        self.environment = environment

    @classmethod
    def find(cls, paths=DEFAULT_CONF_PATHS, env=None):
        """
        Find config from possible filesystem locations.

        'env' is which environment to use from the config: it corresponds to the name of a config section

        :type paths: list[str]
        :type env: str
        :rtype: LocalConfig
        """

        config = compat.read_config(_DEFAULT_CONF)
        files_loaded = config.read(p for p in paths if p)

        env = env or os.environ.get('DATACUBE_ENVIRONMENT') or config.get('user', 'default_environment')
        if not config.has_section(env):
            raise ValueError('No config section found for environment %r' % (env,))

        return LocalConfig(
            config,
            environment=env,
            files_loaded=files_loaded
        )

    def _prop(self, key):
        try:
            return self._config.get(self.environment, key)
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
    def db_username(self):
        try:
            import pwd
            default_username = pwd.getpwuid(os.geteuid()).pw_name
        except ImportError:
            # No default on Windows
            default_username = None

        return self._prop('db_username') or default_username

    @property
    def default_driver(self):
        return self._prop('default_driver')

    @property
    def db_password(self):
        return self._prop('db_password')

    @property
    def db_port(self):
        return self._prop('db_port') or '5432'

    def __str__(self):
        return "LocalConfig<loaded_from={}, config={}, environment={})".format(
            self.files_loaded or 'defaults',
            dict(self._config[self.environment]),
            self.environment
        )

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
