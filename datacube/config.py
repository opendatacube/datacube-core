# coding=utf-8
"""
User configuration.
"""

import os
from pathlib import Path
import configparser
from urllib.parse import unquote_plus, urlparse
from typing import Optional, Iterable, Union, Any, Tuple, Dict

PathLike = Union[str, 'os.PathLike[Any]']


ENVIRONMENT_VARNAME = 'DATACUBE_CONFIG_PATH'
#: Config locations in order. Properties found in latter locations override
#: earlier ones.
#:
#: - `/etc/datacube.conf`
#: - file at `$DATACUBE_CONFIG_PATH` environment variable
#: - `~/.datacube.conf`
#: - `datacube.conf`
DEFAULT_CONF_PATHS = tuple(p for p in ['/etc/datacube.conf',
                                       os.environ.get(ENVIRONMENT_VARNAME, ''),
                                       str(os.path.expanduser("~/.datacube.conf")),
                                       'datacube.conf'] if len(p) > 0)

DEFAULT_ENV = 'default'

# Default configuration options.
_DEFAULT_CONF = """
[DEFAULT]
# Blank implies localhost
db_hostname:
db_database: datacube
index_driver: default
# If a connection is unused for this length of time, expect it to be invalidated.
db_connection_timeout: 60

[user]
# Which environment to use when none is specified explicitly.
#   note: will fail if default_environment points to non-existent section
# default_environment: datacube
"""

#: Used in place of None as a default, when None is a valid but not default parameter to a function
_UNSET = object()


def read_config(default_text: Optional[str] = None) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    if default_text is not None:
        config.read_string(default_text)
    return config


class LocalConfig(object):
    """
    System configuration for the user.

    This loads from a set of possible configuration files which define the available environments.
    An environment contains connection details for a Data Cube Index, which provides access to
    available data.

    """

    def __init__(self, config: configparser.ConfigParser,
                 files_loaded: Optional[Iterable[str]] = None,
                 env: Optional[str] = None):
        """
        Datacube environment resolution precedence is:
          1. Supplied as a function argument `env`
          2. DATACUBE_ENVIRONMENT environment variable
          3. user.default_environment option in the config
          4. 'default' or 'datacube' whichever is present

        If environment is supplied by any of the first 3 methods is not present
        in the config, then throw an exception.
        """
        self._config = config
        self.files_loaded = [] if files_loaded is None else list(iter(files_loaded))

        if env is None:
            env = os.environ.get('DATACUBE_ENVIRONMENT',
                                 config.get('user', 'default_environment', fallback=None))

        # If the user specifies a particular env, we either want to use it or Fail
        if env:
            if config.has_section(env):
                self._env = env
                # All is good
                return
            else:
                raise ValueError('No config section found for environment %r' % (env,))
        else:
            # If an env hasn't been specifically selected, we can fall back defaults
            fallbacks = [DEFAULT_ENV, 'datacube']
            for fallback_env in fallbacks:
                if config.has_section(fallback_env):
                    self._env = fallback_env
                    return
            raise ValueError('No ODC environment, checked configurations for %s' % fallbacks)

    @classmethod
    def find(cls,
             paths: Optional[Union[str, Iterable[PathLike]]] = None,
             env: Optional[str] = None) -> 'LocalConfig':
        """
        Find config from environment variables or possible filesystem locations.

        'env' is which environment to use from the config: it corresponds to the name of a
        config section
        """
        config = read_config(_DEFAULT_CONF)

        if paths is None:
            if env is None:
                env_opts = parse_env_params()
                if env_opts:
                    return _cfg_from_env_opts(env_opts, config)

            paths = DEFAULT_CONF_PATHS

        if isinstance(paths, str) or hasattr(paths, '__fspath__'):  # Use os.PathLike in 3.6+
            paths = [str(paths)]

        files_loaded = config.read(str(p) for p in paths if p)

        return LocalConfig(
            config,
            files_loaded=files_loaded,
            env=env,
        )

    def get(self, item: str, fallback=_UNSET):
        if fallback is _UNSET:
            return self._config.get(self._env, item)
        else:
            return self._config.get(self._env, item, fallback=fallback)

    def __getitem__(self, item: str):
        return self.get(item, fallback=None)

    def __str__(self) -> str:
        return "LocalConfig<loaded_from={}, environment={!r}, config={}>".format(
            self.files_loaded or 'defaults',
            self._env,
            dict(self._config[self._env]),
        )

    def __repr__(self) -> str:
        return str(self)


DB_KEYS = ('hostname', 'port', 'database', 'username', 'password')


def parse_connect_url(url: str) -> Dict[str, str]:
    """ Extract database,hostname,port,username,password from db URL.

    Example: postgresql://username:password@hostname:port/database

    For local password-less db use `postgresql:///<your db>`
    """
    def split2(s: str, separator: str) -> Tuple[str, str]:
        i = s.find(separator)
        return (s, '') if i < 0 else (s[:i], s[i+1:])

    _, netloc, path, *_ = urlparse(url)

    db = path[1:] if path else ''
    if '@' in netloc:
        (user, password), (host, port) = (split2(p, ':') for p in split2(netloc, '@'))
    else:
        user, password = '', ''
        host, port = split2(netloc, ':')

    oo = dict(hostname=host, database=db)

    if port:
        oo['port'] = port
    if password:
        oo['password'] = unquote_plus(password)
    if user:
        oo['username'] = user
    return oo


def parse_env_params() -> Dict[str, str]:
    """
    - Extract parameters from DATACUBE_DB_URL if present
    - Else look for DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DATABASE
    - Return {} otherwise
    """

    db_url = os.environ.get('DATACUBE_DB_URL', None)
    if db_url is not None:
        return parse_connect_url(db_url)

    params = {k: os.environ.get('DB_{}'.format(k.upper()), None)
              for k in DB_KEYS}
    return {k: v
            for k, v in params.items()
            if v is not None and v != ""}


def _cfg_from_env_opts(opts: Dict[str, str],
                       base: configparser.ConfigParser) -> LocalConfig:
    base['default'] = {'db_'+k: v for k, v in opts.items()}
    return LocalConfig(base, files_loaded=[], env='default')


def render_dc_config(params: Dict[str, Any],
                     section_name: str = 'default') -> str:
    """ Render output of parse_env_params to a string that can be written to config file.
    """
    oo = '[{}]\n'.format(section_name)
    for k in DB_KEYS:
        v = params.get(k, None)
        if v is not None:
            oo += 'db_{k}: {v}\n'.format(k=k, v=v)
    return oo


def auto_config() -> str:
    """
    Render config to $DATACUBE_CONFIG_PATH or ~/.datacube.conf, but only if doesn't exist.

    option1:
      DATACUBE_DB_URL  postgresql://user:password@host:port/database

    option2:
      DB_{HOSTNAME|PORT|USERNAME|PASSWORD|DATABASE}

    option3:
       default config
    """
    cfg_path = os.environ.get('DATACUBE_CONFIG_PATH', None)
    cfg_path = Path(cfg_path) if cfg_path else Path.home()/'.datacube.conf'

    if cfg_path.exists():
        return str(cfg_path)

    opts = parse_env_params()

    if len(opts) == 0:
        opts['hostname'] = ''
        opts['database'] = 'datacube'

    cfg_text = render_dc_config(opts)
    with open(str(cfg_path), 'wt') as f:
        f.write(cfg_text)

    return str(cfg_path)
