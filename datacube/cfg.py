# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube configuration
"""
import os
import warnings
from enum import Enum
from itertools import chain

from os import PathLike
from os.path import expanduser
from typing import Any
from urllib.parse import urlparse


class ConfigException(Exception):
    pass


_DEFAULT_CONF = """
default:
   db_hostname: ''
   db_database: datacube
   index_driver: default
   db_connection_timeout: 60
"""

# Use first file in list that exists.

_DEFAULT_CONFIG_SEARCH_PATH = [
    "datacube.conf",      # i.e. in the current working directory.
    expanduser("~/.datacube.conf"),   # i.e. in user's home directory.
    "/etc/default/datacube.conf",  # Preferred location for global config file
    "/etc/datacube.conf",          # Legacy location for global config file
]

_DEFAULT_IAM_TIMEOUT = 600


def find_config(paths_in: None | str | PathLike | list[str | PathLike]) -> str:
    using_default_paths: bool = False
    if paths_in is None:
        paths: list[str | PathLike] = _DEFAULT_CONFIG_SEARCH_PATH
        using_default_paths = True
    elif isinstance(paths_in, str) or isinstance(paths_in, PathLike):
        paths = [paths_in]
    else:
        paths = paths_in

    for path in paths:
        try:
            with open(path, "r") as fp:
                return fp.read()
        except PermissionError:
            continue
        except FileNotFoundError:
            continue

    if using_default_paths:
        warnings.warn("No configuration file found - using default configuration")
        return _DEFAULT_CONF

    raise ConfigException("No configuration file found in the provided locations")


class CfgFormat(Enum):
    AUTO = 0
    INI = 1
    YAML = 2
    JSON = 2   # JSON is a subset of YAML


def smells_like_ini(cfg_text: str):
    for line in cfg_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line[0] in [";", "["]:
            return True
        else:
            return False
    # Doesn't smell like anything
    return False


def parse_text(cfg_text, fmt: CfgFormat = CfgFormat.AUTO) -> dict[str, dict[str, Any]]:
    raw_config = {}
    if fmt == fmt.INI or (
            fmt == fmt.AUTO and smells_like_ini(cfg_text)):
        import configparser
        try:
            ini_config = configparser.ConfigParser()
            ini_config.read_string(cfg_text)
        except configparser.Error as e:
            raise ConfigException(f"Invalid INI file: {e}")
        for section in ini_config.sections():
            sect = {}
            for key, value in ini_config.items(section):
                sect[key] = value
            if section == "DEFAULT":
                # Normalise "DEFAULT" section (which has ini-specific behaviour) to lowercase for internal use.
                section = section.lower()
            raw_config[section] = sect
    else:
        import yaml
        try:
            raw_config = yaml.load(cfg_text, Loader=yaml.Loader)
        except yaml.parser.ParserError as e:
            raise ConfigException(f"Invalid YAML file:{e}")

    return raw_config


class ODCConfig:
    def __init__(
            self,
            paths: None | str | PathLike | list[str | PathLike] = None,
            text: str | None = None):
        """
        Configuration reader/parser.

        :param paths: Optional
        :param text:
        """
        # Cannot supply both text AND paths.
        if text is not None and paths is not None:
            raise ConfigException("Cannot supply both configuration path(s) and explicit configuration text.")

        # Only suppress environment variable overrides if explicit config text is supplied.
        self.allow_envvar_overrides = text is None

        if text is None:
            text = find_config(paths)

        self.raw_text: str = text
        self.raw_config: dict[str, dict[str, Any]] = parse_text(self.raw_text)

        self.aliases = {}
        self.known_environments = {
            section: ODCEnvironment(self, section, self.raw_config[section], self.allow_envvar_overrides)
            for section in self.raw_config
        }
        for alias, canonical in self.aliases.items():
            self.known_environments[alias] = self[canonical]

    def add_alias(self, alias, canonical):
        self.aliases[alias] = canonical

    def __getitem__(self, item):
        if item not in self.known_environments:
            self.known_environments[item] = ODCEnvironment(self, item, {}, True)
        return self.known_environments[item]


# dataclassify?
class ODCOptionHandler:
    allow_envvar_lookup: bool = True

    def __init__(self, name: str, env: "ODCEnvironment", default: Any = None,
                 env_aliases=None,
                 legacy_env_aliases=None):
        self.name: str = name
        self.env: "ODCEnvironment" = env
        self.default: Any = default
        if env_aliases:
            self.env_aliases = env_aliases
        else:
            self.env_aliases = []
        if legacy_env_aliases:
            self.legacy_env_aliases = legacy_env_aliases
        else:
            self.legacy_env_aliases = []

    def validate_and_normalise(self, value: Any) -> Any:
        if self.default is not None and value is None:
            return self.default
        return value

    def handle_dependent_options(self, value: Any) -> None:
        pass

    def get_val_from_environment(self) -> str | None:
        if self.allow_envvar_lookup and self.env._allow_envvar_overrides:
            canonical_name = (f"odc_{self.env._name}_{self.name}".upper())
            for env_name in chain([canonical_name], self.env_aliases):
                val = os.environ.get(env_name)
                if val is not None:
                    return val
            for env_name in self.legacy_env_aliases:
                val = os.environ.get(env_name)
                if val is not None:
                    warnings.warn(
                        f"Config being passed in by legacy environment variable ${env_name}. "
                        f"Please use ${canonical_name} instead.")
                    return val
        return None


class AliasOptionHandler(ODCOptionHandler):
    allow_envvar_lookup: bool = False

    def validate_and_normalise(self, value: Any) -> Any:
        if value is None:
            return None
        raise ConfigException("Illegal attempt to directly access alias environment"
                              " - use the ODCConfig object to resolve the environment")


class IndexDriverOptionHandler(ODCOptionHandler):
    def validate_and_normalise(self, value: Any) -> Any:
        value = super().validate_and_normalise(value)
        from datacube.drivers.indexes import index_drivers
        if value not in index_drivers():
            raise ConfigException(f"Unknown index driver: {value} - Try one of {','.join(index_drivers())}")
        return value

    def handle_dependent_options(self, value: Any) -> None:
        # Get driver-specific config options
        from datacube.drivers.indexes import index_driver_by_name
        driver = index_driver_by_name(value)
        for option in driver.get_config_option_handlers(self.env):
            self.env._option_handlers.append(option)


# dataclassify?
class ODCEnvironment:
    def __init__(self,
                 cfg: ODCConfig,
                 name: str,
                 raw: dict[str, Any],
                 allow_env_overrides: bool = True):
        self._cfg: ODCConfig = cfg
        self._name: str = name
        self._raw: dict[str, Any] = raw
        self._allow_envvar_overrides: bool = allow_env_overrides
        self._normalised: dict[str, Any] = {}

        if "alias" in self._raw:
            self._cfg.add_alias(self._name, self._raw["alias"])

        self._option_handlers: list[ODCOptionHandler] = [
            AliasOptionHandler("alias", self),
            IndexDriverOptionHandler("index_driver", self, default="default")
        ]

    def __getitem__(self, key: str) -> Any:
        if not self._normalised:
            # First access of environment - process config
            # Loop through content handlers.
            # Note that handlers may add more handlers to the end of the list while we are iterating over it.
            for handler in self._option_handlers:
                self._handle_option(handler)

        # Config already processed
        # 1. From Normalised
        if key in self._normalised:
            return self._normalised[key]
        # 2. from Environment variables (if allowed)
        if self._allow_envvar_overrides:
            try:
                return os.environ[key]
            except KeyError:
                pass
        # 3. from raw config
        if key in self._raw:
            return self._raw[key]
        # No config, no default.
        raise KeyError(key)

    def __getattr__(self, item: str) -> Any:
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)

    def _handle_option(self, handler: ODCOptionHandler) -> None:
        val = handler.get_val_from_environment()
        if not val:
            val = self._raw.get(handler.name)
        val = handler.validate_and_normalise(val)
        self._normalised[handler.name] = val
        handler.handle_dependent_options(val)


# Config options for shared use by postgres-based index drivers (i.e. postgres and postgis drivers)
class PostgresURLOptionHandler(ODCOptionHandler):
    def validate_and_normalise(self, value: Any) -> Any:
        if not value:
            return None
        components = urlparse(value)
        # Check URL scheme is postgresql:
        if components.scheme != "postgresql":
            raise ConfigException("Database URL is not a postgresql connection URL")
        # Don't bother splitting up the url, we'd just have to put it back together again later
        return value

    def handle_dependent_options(self, value: Any) -> None:
        if value is None:
            for handler in (
                    ODCOptionHandler("db_username", self.env, env_aliases=['DB_USERNAME']),
                    ODCOptionHandler("db_password", self.env, env_aliases=['DB_PASSWORD']),
                    ODCOptionHandler("db_hostname", self.env, env_aliases=['DB_HOSTNAME']),
                    IntOptionHandler("db_port", self.env, default=5432, env_aliases=['DB_PORT'],
                                     minval=1, maxval=49151),
                    ODCOptionHandler("db_database", self.env, env_aliases=['DB_DATABASE']),
            ):
                self.env._option_handlers.append(handler)


class IntOptionHandler(ODCOptionHandler):
    def __init__(self, *args, minval: int | None = None, maxval: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.minval = minval
        self.maxval = maxval

    def validate_and_normalise(self, value: Any) -> Any:
        value = super().validate_and_normalise(value)
        try:
            ival = int(value)
        except ValueError:
            raise ConfigException(f"Config option {self.name} must be an integer")
        if self.minval is not None and ival < self.minval:
            raise ConfigException(f"Config option {self.name} must be at least {self.minval}")
        if self.maxval is not None and ival > self.maxval:
            raise ConfigException(f"Config option {self.name} must not be greater than {self.minval}")
        return ival


class IAMAuthenticationOptionHandler(ODCOptionHandler):
    def validate_and_normalise(self, value: Any) -> Any:
        if isinstance(value, bool):
            return value
        elif isinstance(value, str) and value.lower() in ('y', 'yes'):
            return True
        else:
            return False

    def handle_dependent_options(self, value: Any) -> None:
        if value:
            self.env._option_handlers.append(
                IntOptionHandler("db_iam_timeout", self.env, default=_DEFAULT_IAM_TIMEOUT,
                                 env_aliases=['DATACUBE_IAM_TIMEOUT'],
                                 minval=1)
            )


def config_options_for_psql_driver(env: ODCEnvironment):
    return [
        PostgresURLOptionHandler("db_url", env,
                                 env_aliases=['DATACUBE_DB_URL']),
        IAMAuthenticationOptionHandler("db_iam_authentication", env,
                                       env_aliases=['DATACUBE_IAM_AUTHENTICATION']),
        IntOptionHandler("db_connection_timeout", env, default=60, minval=1)
    ]
