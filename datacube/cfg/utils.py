# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import re
from collections.abc import Callable
from typing import Any, TypeAlias

from .exceptions import ConfigException

# A raw configuration dictionary. A dictionary of dictionaries
ConfigDict: TypeAlias = dict[str, dict[str, Any]]
SemaphoreCallback = Callable[[], None]


def check_valid_env_name(name: str) -> None:
    """
    Enforce a valid ODC environment name.

    :param name: Candidate name
    :return:  None (raises ConfigException if invalid)
    """
    if not re.fullmatch(r"^[a-z][a-z0-9]*$", name):
        raise ConfigException(
            f"Environment names must consist of only lower case letters and numbers: {name}"
        )
    if name.lower() == "all":
        raise ConfigException('Environments cannot be named "ALL"')


def check_valid_option(name: str) -> None:
    """
    Enforce a valid ODC config option name.

    :param name: Candidate name
    :return:  None (raises ConfigException if invalid)
    """
    if not re.fullmatch(r"^[a-z][a-z_]*$", name):
        raise ConfigException(
            f"Config option names must consist of only lower case letters, numbers and underscores: {name}"
        )


def smells_like_ini(cfg_text: str) -> bool:
    """
    Does this file smell like an INI file?

    (Is the first non-whitespace character a '[' or ';'?)

    :param cfg_text: The contents of the file to be smelled
    :return: True if it smells like an ini file.
    """
    for line in cfg_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if line[0] in [";", "["]:
            return True
        else:
            return False
    # Empty file - parse as ini so we end up with the right high level structure at least
    return True
