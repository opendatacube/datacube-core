# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Datacube configuration
"""

import os
import warnings
from os import PathLike
from threading import Lock
from typing import Any, TypeAlias, Union, cast

from ..migration import ODC2DeprecationWarning
from .cfg import find_config, parse_text
from .exceptions import ConfigException
from .opt import (
    AliasOptionHandler,
    BoolOptionHandler,
    IndexDriverOptionHandler,
    IntOptionHandler,
    ODCOptionHandler,
)
from .utils import ConfigDict, check_valid_env_name

# TypeAliases for more concise type hints
# (Unions required as typehint | operator doesn't work with string forward-references).
GeneralisedPath: TypeAlias = str | PathLike | list[str | PathLike]
GeneralisedCfg: TypeAlias = Union["ODCConfig", GeneralisedPath]
GeneralisedEnv: TypeAlias = Union["ODCEnvironment", str]
GeneralisedRawCfg: TypeAlias = str | ConfigDict


class ODCConfig:
    """
    Configuration finder/reader/parser.

    **Attributes**

    .. py:attribute:: allow_envvar_overrides
       :value: True

       If True, environment variables can override the values explicitly specified in the supplied configuration.

       Note that environments not explicitly specified in the supplied configuration (dynamic environments) can
       still be read from environment variables, even if this attribute is False.

    .. py:attribute:: raw_text

       The raw configuration text being used, as read from the configuration
       file or supplied directly by the user.  May be None if the user
       directly supplied configuration as a dictionary. May be in ini or yaml
       format.  Does not include dynamic environments or values overridden by
       environment variables.

    .. py:attribute:: raw_config

       The raw dictionary form of the configuration, as supplied directly
       by the user, or as parsed from raw_text. Does not include dynamic
       environments or values overridden by environment variables.

    .. py:attribute:: known_environments

       A dictionary containing all environments defined in raw_config,
       plus any dynamic environments read so far.
       Environment themselves are not validated until read from.

    .. py:attribute:: canonical_names

       A dictionary mapping canonical environment names to all aliases for
       that environment.
    """

    allow_envvar_overrides: bool = True
    raw_text: str | None = None
    raw_config: ConfigDict = {}
    known_environments: dict[str, "ODCEnvironment"] = {}
    canonical_names: dict[str, list[str]] = {}
    is_default = False

    def __init__(
        self,
        paths: GeneralisedPath | None = None,
        raw_dict: ConfigDict | None = None,
        text: str | None = None,
    ) -> None:
        """
        When called with no args, reads the first config file found in the config path list is used.
        The config path list is taken from:

        1) Environment variable $ODC_CONFIG_PATH (as a UNIX path style colon-separated path list)
        2) Environment variable $DATACUBE_CONFIG_PATH (as a UNIX path style colon-separated path list)
           This is a deprecated legacy environment variable, and please note that it's behaviour has changed
           slightly from datacube 1.8.x.
        3) The default config search path (i.e. .cfg._DEFAULT_CONFIG_SEARCH_PATH)

        If no config file is found at any of the paths in active path list, use the default configuration
        at , or if no such config file exists, use the default configuration (.cfg._DEFAULT_CONF). Configuration
        files may be in ini or yaml format. Environment variable overrides ARE applied.

        Otherwise, user may supply one (and only one) of the following:

        :param paths: The path of the configuration file, or a list of paths of candidate configuration files (the
                      first in the list that can be read is used).  If none of the supplied paths can be read, an
                      error is raised.  (Unlike calling with no arguments, the fallback default config is NOT
                      used.) Configuration file may be in ini or yaml format. Environment variable overrides ARE
                      applied.
        :param raw_dict: A raw dictionary containing configuration data.
                         Used as is - environment variable overrides are NOT applied.
        :param text: A string containing configuration data in ini or yaml format.
                     Used as is - environment variable overrides are NOT applied.
        """
        # Cannot supply both text AND paths.
        args_supplied: int = sum(int(bool(x)) for x in (paths, raw_dict, text))
        if args_supplied > 1:
            raise ConfigException(
                "Can only supply one of configuration path(s), raw dictionary, "
                "and explicit configuration text."
            )

        # Suppress environment variable overrides if explicit config text or dictionary is supplied.
        self.allow_envvar_overrides = not text and not raw_dict

        if not raw_dict and not text:
            # No explicit config passed in.  Check for ODC_CONFIG environment variables
            if os.environ.get("ODC_CONFIG"):
                text = os.environ["ODC_CONFIG"]
            else:
                # Read config text from config file
                text = find_config(paths, default_cb=self._set_default)

        self.raw_text = text
        if raw_dict is not None:
            self.raw_config = raw_dict
        else:
            self.raw_config = parse_text(cast(str, self.raw_text))

        self._aliases: dict[str, str] = {}
        self.known_environments: dict[str, ODCEnvironment] = {
            section: ODCEnvironment(
                self, section, self.raw_config[section], self.allow_envvar_overrides
            )
            for section in self.raw_config
        }
        self.canonical_names: dict[str, list[str]] = {}
        for alias, canonical in self._aliases.items():
            self.known_environments[alias] = self[canonical]
            if canonical in self.canonical_names:
                self.canonical_names[canonical].append(alias)
            else:
                self.canonical_names[canonical] = [canonical, alias]

    @classmethod
    def get_environment(
        cls,
        env: GeneralisedEnv | None = None,
        config: GeneralisedCfg | None = None,
        raw_config: GeneralisedRawCfg | None = None,
    ) -> "ODCEnvironment":
        """
        Obtain an ODCConfig object from the most general possible arguments.

        It is an error to supply both config and raw_config, otherwise everything is optional and
        honours system defaults.

        :param env: An ODCEnvironment object or a string.
        :param config: An ODCConfig object or a config path.
        :param raw_config: A raw config string or ConfigDict.
        :return:
        """
        if config is not None and raw_config is not None:
            raise ConfigException("Cannot specify both config and raw_config")
        if isinstance(env, ODCEnvironment):
            return env
        else:
            if isinstance(config, ODCConfig):
                cfg = config
            elif isinstance(raw_config, str):
                cfg = ODCConfig(paths=config, text=raw_config)
            else:
                cfg = ODCConfig(paths=config, raw_dict=raw_config)
            return cfg[env]

    def _add_alias(self, alias: str, canonical: str) -> None:
        """
        Register an environment alias during ODCConfig class construction.

        Used internally by the Configuration library during class initialisation. Has no effect after initialisation.

        :param alias: The alias for the environment
        :param canonical: The canonical environment name the alias refers to
        """
        self._aliases[alias] = canonical

    def get_aliases(self, canonical_name: str) -> list[str]:
        """
        Get list of all possible names for a given canonical name.

        :param canonical_name: The canonical name of the target environment
        :return: A list of all known names for the target environment, including the canonical name itself and
                 any aliases.
        """
        if canonical_name in self.canonical_names:
            return self.canonical_names[canonical_name]
        else:
            return [canonical_name]

    def _set_default(self) -> None:
        self.is_default = True

    def __getitem__(self, item: str | None) -> "ODCEnvironment":
        """
        Environments can be accessed by name (canonical or aliases) with the getitem dunder method.

        E.g. cfg["env"]

        Passing in None returns the default environment, which is the first resolvable environment of:

        1. The environment named by the $ODC_ENVIRONMENT variable
        2. The environment named by the $DATACUBE_ENVIRONMENT variable
           (legacy environment variable name - now deprecated)
        3. The environment called `default` (dynamic environment lookup not supported)
        4. The environment called `datacube` (dynamic environment lookup not supported)

        If none of the above environment names are known, then a ConfigException is raised.

        If an explicit environment name is passed in that does not exist, dynamic environment lookup is attempted.
        Dynamic environment lookup always succeeds, but may return an environment which cannot connect to a database.

        :param item: A canonical environment name, an environment alias, or None.
        :return: A ODCEnvironment object
        """
        if item is None:
            # Get default.
            if os.environ.get("ODC_ENVIRONMENT"):
                item = os.environ["ODC_ENVIRONMENT"]
            elif os.environ.get("DATACUBE_ENVIRONMENT"):
                warnings.warn(
                    "Setting the default environment with $DATACUBE_ENVIRONMENT is deprecated. "
                    "Please use $ODC_ENVIRONMENT instead.",
                    ODC2DeprecationWarning,
                )
                item = os.environ["DATACUBE_ENVIRONMENT"]
            elif "default" in self.known_environments:
                item = "default"
            elif "datacube" in self.known_environments:
                warnings.warn(
                    "Defaulting to the 'datacube' environment - "
                    "this fallback behaviour is deprecated and may change in a future release.",
                    ODC2DeprecationWarning,
                )
                item = "datacube"
            else:
                # No explicitly defined (known) environments - assume default and hope there's config
                # available in environment variables.
                item = "default"
        if item not in self.known_environments:
            self.known_environments[item] = ODCEnvironment(self, item, {}, True)
        return self.known_environments[item]


class ODCEnvironment:
    """
    Configuration reader for an individual ODC environment.

    Only configuration options with a registered option handler are able to be read.  Configuration options
    may be read either as attributes on the ODCEnvironment objects or via the getitem dunder method.

    E.g.    env.index_driver   or    env["index_driver"]

    Attempting to access an unhandled or invalid option will raise a KeyError or AttributeError, as
    appropriate for the access method.

    ODCEnvironment objects should only be instantiated by and acquired from an ODCConfig object.
    """

    def __init__(
        self,
        cfg: ODCConfig,
        name: str,
        raw: dict[str, Any],
        allow_env_overrides: bool = True,
    ) -> None:
        self._cfg: ODCConfig = cfg
        check_valid_env_name(name)
        self._name: str = name
        self._raw: dict[str, Any] = raw
        self._allow_envvar_overrides: bool = allow_env_overrides
        self._lock = Lock()
        self._normalised: dict[str, Any] = {}

        if name == "user" and "default_environment" in raw:
            warnings.warn(
                "The 'default_environment' setting in the 'user' section is no longer supported - "
                "please refer to the documentation for more information"
            )

        self._env_overrides_applied = False
        # Aliases are handled here, the alias OptionHandler is a place-holder.
        if "alias" in self._raw:
            alias = self._raw["alias"]
            check_valid_env_name(alias)
            self._cfg._add_alias(self._name, alias)
            for opt in self._raw.keys():
                if opt != "alias":
                    raise ConfigException(
                        f"Alias environments should only contain an alias option. Extra option {opt} found."
                    )

        self._option_handlers: list[ODCOptionHandler] = [
            AliasOptionHandler("alias", self),
            IndexDriverOptionHandler("index_driver", self, default="default"),
            BoolOptionHandler("skip_broken_datasets", self, default=False),
            IntOptionHandler("dc_load_limit", self, minval=0),
        ]

    def get_all_aliases(self) -> list[str]:
        return self._cfg.get_aliases(self._name)

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            if not self._normalised:
                # First access of environment - process config
                # Loop through content handlers.
                # Note that handlers may add more handlers to the end of the list while we are iterating over it.
                for handler in self._option_handlers:
                    self._handle_option(handler)
                if self._cfg.is_default and not self._env_overrides_applied:
                    warnings.warn(
                        "No configuration file found - using default configuration and environment variables"
                    )

        # Config already processed
        # 1. From Normalised
        if key in self._normalised:
            return self._normalised[key]
        # No config, no default.
        raise KeyError(key)

    def __getattr__(self, item: str) -> Any:
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item) from None

    def _handle_option(self, handler: ODCOptionHandler) -> None:
        val = handler.get_val_from_environment()
        if val:
            self._env_overrides_applied = True
        else:
            val = self._raw.get(handler.name)
        val = handler.validate_and_normalise(val)
        self._normalised[handler.name] = val
        handler.handle_dependent_options(val)
