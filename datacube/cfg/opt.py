# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import warnings
from typing import Any, TYPE_CHECKING
from urllib.parse import urlparse
from urllib.parse import quote_plus

from .exceptions import ConfigException
from .utils import check_valid_option

if TYPE_CHECKING:
    from .api import ODCEnvironment


_DEFAULT_IAM_TIMEOUT = 600
_DEFAULT_CONN_TIMEOUT = 60
_DEFAULT_HOSTNAME = 'localhost'
_DEFAULT_DATABASE = 'datacube'

try:
    import pwd

    _DEFAULT_DB_USER = pwd.getpwuid(os.geteuid()).pw_name  # type: Optional[str]
except (ImportError, KeyError):
    # No default on Windows and some other systems
    _DEFAULT_DB_USER = None


class ODCOptionHandler:
    """
    Base option handler class.  Sufficient for basic option with no validation.

    Smarter or more specialised option handlers extend this class.
    """
    # If overridden by subclass to False, then no environment variable overrides apply.
    allow_envvar_lookup: bool = True

    def __init__(self, name: str, env: "ODCEnvironment", default: Any = None,
                 legacy_env_aliases=None):
        """

        :param name: Name of the option
        :param env: The ODCEnvironment the option is being read from
        :param default: The default value if not specified in the config file
        :param legacy_env_aliases:  Any legacy (pre odc-1.9) environment variable aliases for the option
        """
        check_valid_option(name)
        self.name: str = name
        self.env: "ODCEnvironment" = env
        self.default: Any = default
        if legacy_env_aliases:
            self.legacy_env_aliases = legacy_env_aliases
        else:
            self.legacy_env_aliases = []

    def validate_and_normalise(self, value: Any) -> Any:
        """
        Given a value read from a raw dictionary, return a normalised form.

        Subclasses should replace and call this implementation through super() for default handling.
        :param value: The value read from the raw dictionary (None if not present)
        :return: the normalised value.
        """
        if self.default is not None and value is None:
            return self.default
        return value

    def handle_dependent_options(self, value: Any) -> None:
        """
        Default implementation is no-op

        In subclasses:
        If the value of this option implies that dependent OptionHandlers should be run, they
        should be constructed here, and appended to self.env._option_handlers  (See examples below)

        :param value: The normalised value, as returned by validate_and_normalise
        :return: None
        """
        pass

    def get_val_from_environment(self) -> str | None:
        """
        Handle environment lookups.

        1. Returns None unless self.allow_envvar_lookups and self.env._allow_envvar_overrides are both set.
        2. Check canonical envvar name first.  E.g. option "bar" in environment "foo" -> $ODC_FOO_BAR
        3. Check canonical envvar name for any alias environments that point to this one.
        4. Check any legacy envvar names, and raise warnings if found.

        :return: First environment variable with non-empty value, or None if none found.
        """
        if self.allow_envvar_lookup and self.env._allow_envvar_overrides:
            canonical_name = f"odc_{self.env._name}_{self.name}".upper()
            for env_name in self.env.get_all_aliases():
                envvar_name = f"odc_{env_name}_{self.name}".upper()
                val = os.environ.get(envvar_name)
                if val:
                    return val
            for env_name in self.legacy_env_aliases:
                val = os.environ.get(env_name)
                if val:
                    warnings.warn(
                        f"Config being passed in by legacy environment variable ${env_name}. "
                        f"Please use ${canonical_name} instead.")
                    return val
        return None


class AliasOptionHandler(ODCOptionHandler):
    """
    Alias option is handled at the environment level.
    """
    allow_envvar_lookup: bool = False

    def validate_and_normalise(self, value: Any) -> Any:
        if value is None:
            return None
        raise ConfigException("Illegal attempt to directly access alias environment"
                              " - use the ODCConfig object to resolve the environment")


class IndexDriverOptionHandler(ODCOptionHandler):
    """
    Index Driver option.  Must be a valid index driver name, and index drivers may support further configuration.

    Example implementation for Postgresql/Postgis-based index drivers shown below.
    """
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


class IntOptionHandler(ODCOptionHandler):
    """
    Require an integer value, with optional min and max vals.
    """
    def __init__(self, *args, minval: int | None = None, maxval: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.minval = minval
        self.maxval = maxval

    def validate_and_normalise(self, value: Any) -> Any:
        # Call super() to get handle default value
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
    """
    A simple boolean, compatible with the historic behaviour of the IAM Authentication on/off option.

    y/yes (case-insensitive): True
    Anything else: False

    If true, adds an IAM Timeout Option to the Environment.
    """
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
                                 legacy_env_aliases=['DATACUBE_IAM_TIMEOUT'],
                                 minval=1)
            )


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
                    ODCOptionHandler("db_username", self.env, legacy_env_aliases=['DB_USERNAME'],
                                     default=_DEFAULT_DB_USER),
                    ODCOptionHandler("db_password", self.env, legacy_env_aliases=['DB_PASSWORD']),
                    ODCOptionHandler("db_hostname", self.env, legacy_env_aliases=['DB_HOSTNAME'],
                                     default=_DEFAULT_HOSTNAME),
                    IntOptionHandler("db_port", self.env, default=5432, legacy_env_aliases=['DB_PORT'],
                                     minval=1, maxval=49151),
                    ODCOptionHandler("db_database", self.env, legacy_env_aliases=['DB_DATABASE'],
                                     default=_DEFAULT_DATABASE),
            ):
                self.env._option_handlers.append(handler)


def config_options_for_psql_driver(env: "ODCEnvironment"):
    """
       Config options for shared use by postgres-based index drivers
       (i.e. postgres and postgis drivers)
    """
    return [
        PostgresURLOptionHandler("db_url", env,
                                 legacy_env_aliases=['DATACUBE_DB_URL']),
        IAMAuthenticationOptionHandler("db_iam_authentication", env,
                                       legacy_env_aliases=['DATACUBE_IAM_AUTHENTICATION']),
        IntOptionHandler("db_connection_timeout", env, default=_DEFAULT_CONN_TIMEOUT, minval=1)
    ]


def psql_url_from_config(env: "ODCEnvironment"):
    if env.db_url:
        return env.db_url
    if not env.db_database:
        raise ConfigException(f"No database name supplied for environment {env._name}")
    url = "postgresql://"
    if env.db_username:
        if env.db_password:
            escaped_password = quote_plus(env.db_password)
            url += f"{env.db_username}:{escaped_password}@"
        else:
            url += f"{env.db_username}@"
    url += f"{env.db_hostname}:{env.db_port}/{env.db_database}"
    return url
