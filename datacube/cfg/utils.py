# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import re
from .exceptions import ConfigException


def check_valid_env_name(name: str) -> None:
    if not re.fullmatch(r"^[a-z][a-z0-9]*$", name):
        raise ConfigException(f'Environment names must consist of only lower case letters and numbers: {name}')


def check_valid_field_name(name: str) -> None:
    if not re.fullmatch(r"^[a-z][a-z_]*$", name):
        raise ConfigException(
            f'Config option names must consist of only lower case letters, numbers and underscores: {name}'
        )


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
