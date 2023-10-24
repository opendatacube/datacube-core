# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import warnings
from enum import Enum
from os import PathLike
from os.path import expanduser
from typing import Any

from datacube.cfg.exceptions import ConfigException
from datacube.cfg.utils import smells_like_ini

_DEFAULT_CONFIG_SEARCH_PATH = [
    "datacube.conf",      # i.e. in the current working directory.
    expanduser("~/.datacube.conf"),   # i.e. in user's home directory.
    "/etc/default/datacube.conf",  # Preferred location for global config file
    "/etc/datacube.conf",          # Legacy location for global config file
]
_DEFAULT_CONF = """
default:
   db_hostname: ''
   db_database: datacube
   index_driver: default
   db_connection_timeout: 60
"""


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


def parse_text(cfg_text, fmt: CfgFormat = CfgFormat.AUTO) -> dict[str, dict[str, Any]]:
    raw_config = {}
    if fmt == fmt.INI or (
            fmt == fmt.AUTO and smells_like_ini(cfg_text)):
        import configparser
        try:
            ini_config = configparser.ConfigParser()
            ini_config.read_string(cfg_text)
            for section in ini_config.sections():
                sect = {}
                for key, value in ini_config.items(section):
                    sect[key] = value

                raw_config[section] = sect
        except configparser.Error as e:
            raise ConfigException(f"Invalid INI file: {e}")
    else:
        import yaml
        try:
            raw_config = yaml.load(cfg_text, Loader=yaml.Loader)
        except yaml.parser.ParserError as e:
            raise ConfigException(f"Invalid YAML file:{e}")

    return raw_config
