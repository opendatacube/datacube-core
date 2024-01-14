# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Low level config path resolution, loading and multi-format parsing functions.

The default search path and default config also live here.
"""

import os
import warnings
from enum import Enum
from os import PathLike
from os.path import expanduser

from datacube.cfg.exceptions import ConfigException
from datacube.cfg.utils import ConfigDict, smells_like_ini

_DEFAULT_CONFIG_SEARCH_PATH = [
    "datacube.conf",      # i.e. in the current working directory.
    expanduser("~/.datacube.conf"),   # i.e. in user's home directory.
    # Check if we are running under a Windows and use Windowsy default paths?
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
    """
    Given a file system path, or a list of file system paths, return the contents of the first file
    in the list that can be read as a string.

    If "None" is passed in the default config search path is used:

        "datacube.conf"               (i.e. 'datacube.conf' in the current working directory.)
        "~/.datacube.conf"            (i.e. '.datacube.conf' in the user's home directory.)
        "/etc/default/datacube.conf"  (Preferred location for global config file)
        "/etc/datacube.conf"          (Legacy location for global config file)

    If a path or list of paths was passed in, AND no readable file could be found, a ConfigException is raised.
    If None was passed in, AND no readable file could be found, a default configuration text is returned.

    :param paths_in: A file system path, or a list of file system paths, or None.
    :return: The contents of the first readable file found.
    """
    using_default_paths: bool = False
    if paths_in is None:
        if os.environ.get("ODC_CONFIG_PATH"):
            paths: list[str | PathLike] = os.environ["ODC_CONFIG_PATH"].split(':')
        elif os.environ.get("DATACUBE_CONFIG_PATH"):
            warnings.warn(
                "Datacube config path being determined by legacy $DATACUBE_CONFIG_PATH environment variable. "
                "This environment variable is deprecated and the behaviour of it has changed somewhat since datacube "
                "1.8.x.   Please refer to the documentation for details and switch to $ODC_CONFIG_PATH"
            )
            paths = os.environ["DATACUBE_CONFIG_PATH"].split(':')
        else:
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
    """
    An Enum class for config file formats.
    """
    AUTO = 0   # Format unspecified - autodetect
    INI = 1
    YAML = 2
    JSON = 2   # JSON is a subset of YAML


def parse_text(cfg_text: str, fmt: CfgFormat = CfgFormat.AUTO) -> ConfigDict:
    """
    Parse a string of text in INI, JSON or YAML format into a raw dictionary.

    Raises a ConfigException if the file cannot be parsed.

    :param cfg_text: Configuration string in INI, YAML or JSON format
    :param fmt: Whether to use the ini or yaml/json parser. By default autodetects file format.
    :return: A raw config dictionary
    """
    raw_config = {}
    if fmt == fmt.INI or (
            fmt == fmt.AUTO and smells_like_ini(cfg_text)):
        # INI parsing
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
        # YAML/JSON parsing
        import yaml
        try:
            raw_config = yaml.load(cfg_text, Loader=yaml.Loader)
        except yaml.parser.ParserError as e:
            raise ConfigException(f"Invalid YAML file:{e}")

    return raw_config
