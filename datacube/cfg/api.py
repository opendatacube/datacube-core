# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Datacube configuration
"""
import os
import warnings

from os import PathLike
from typing import Any

from .cfg import find_config, parse_text
from .exceptions import ConfigException
from .opt import ODCOptionHandler, AliasOptionHandler, IndexDriverOptionHandler
from .utils import check_valid_env_name


class ODCConfig:
    def __init__(
            self,
            paths: None | str | PathLike | list[str | PathLike] = None,
            raw_dict: None | dict[str, dict[str, Any]] = None,
            text: str | None = None):
        """
        Configuration finder/reader/parser.

        :param paths: Optional
        :param text:
        """
        # Cannot supply both text AND paths.
        args_supplied: int = int(paths is not None) + int(raw_dict is not None) + int(text is not None)
        if args_supplied > 1:
            raise ConfigException("Can only supply one of configuration path(s), raw dictionary, "
                                  "and explicit configuration text.")

        # Only suppress environment variable overrides if explicit config text is supplied.
        self.allow_envvar_overrides: bool = text is None

        if raw_dict is None and text is None:
            text = find_config(paths)

        self.raw_text: str | None = text
        self.raw_config: dict[str, dict[str, Any]] = raw_dict
        if self.raw_config is None:
            self.raw_config = parse_text(self.raw_text)

        self.aliases: dict[str, str] = {}
        self.known_environments: dict[str, ODCEnvironment] = {
            section: ODCEnvironment(self, section, self.raw_config[section], self.allow_envvar_overrides)
            for section in self.raw_config
        }
        self.canonical_names: dict[str, list[str]] = {}
        for alias, canonical in self.aliases.items():
            self.known_environments[alias] = self[canonical]
            if canonical in self.canonical_names:
                self.canonical_names[canonical].append(alias)
            else:
                self.canonical_names[canonical] = [canonical, alias]

    def add_alias(self, alias, canonical):
        self.aliases[alias] = canonical

    def get_aliases(self, canonical_name: str) -> list[str]:
        if canonical_name in self.canonical_names:
            return self.canonical_names[canonical_name]
        else:
            return [canonical_name]

    def __getitem__(self, item):
        if item is None:
            # Get default.
            if os.environ.get("ODC_ENVIRONMENT"):
                item = os.environ["ODC_ENVIRONMENT"]
            elif os.environ.get("DATACUBE_ENVIRONMENT"):
                warnings.warn(
                    "Setting the default environment with $DATACUBE_ENVIRONMENT is deprecated. "
                    "Please use $ODC_ENVIRONMENT instead.")
                item = os.environ["DATACUBE_ENVIRONMENT"]
            elif "default" in self.known_environments:
                item = "default"
            elif "datacube" in self.known_environments:
                warnings.warn("Defaulting to the 'datacube' environment - "
                              "this fallback behaviour is deprecated and may change in a future release.")
                item = "datacube"
            else:
                raise ConfigException("No environment specified and no default environment could be identified.")
        if item not in self.known_environments:
            self.known_environments[item] = ODCEnvironment(self, item, {}, True)
        return self.known_environments[item]


class ODCEnvironment:
    def __init__(self,
                 cfg: ODCConfig,
                 name: str,
                 raw: dict[str, Any],
                 allow_env_overrides: bool = True):
        self._cfg: ODCConfig = cfg
        check_valid_env_name(name)
        self._name: str = name
        self._raw: dict[str, Any] = raw
        self._allow_envvar_overrides: bool = allow_env_overrides
        self._normalised: dict[str, Any] = {}

        if "alias" in self._raw:
            alias = self._raw['alias']
            check_valid_env_name(alias)
            self._cfg.add_alias(self._name, alias)

        self._option_handlers: list[ODCOptionHandler] = [
            AliasOptionHandler("alias", self),
            IndexDriverOptionHandler("index_driver", self, default="default")
        ]

    def get_all_aliases(self):
        return self._cfg.get_aliases(self._name)

    def __getitem__(self, key: str) -> Any:
        if not self._normalised:
            # First access of environment - process config
            # Loop through content handlers.
            # Note that handlers may add more handlers to the end of the list while we are iterating over it.
            for handler in self._option_handlers:
                print(f"Handling option {handler.name}")
                self._handle_option(handler)

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
            raise AttributeError(item)

    def _handle_option(self, handler: ODCOptionHandler) -> None:
        val = handler.get_val_from_environment()
        if not val:
            val = self._raw.get(handler.name)
        val = handler.validate_and_normalise(val)
        self._normalised[handler.name] = val
        handler.handle_dependent_options(val)
