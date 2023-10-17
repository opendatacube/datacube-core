# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Datacube configuration
"""
import os

from os import PathLike
from typing import Any

from .cfg import find_config, parse_text
from .exceptions import ConfigException
from .opt import ODCOptionHandler, AliasOptionHandler, IndexDriverOptionHandler


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
