# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
try:
    raise ImportError("Disabled until #2430 is resolved")
    from orjson import JSONDecodeError, dump, dumps, load, loads

    using_orjson = True
except ImportError:
    from json import JSONDecodeError, dump, dumps, load, loads

    using_orjson = False

__all__ = [
    "JSONDecodeError",
    "dump",
    "dumps",
    "load",
    "loads",
    "using_orjson",
]
