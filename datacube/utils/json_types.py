# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Module for JSON type aliases.
"""

from __future__ import annotations

from typing import TypeAlias

JsonAtom: TypeAlias = None | bool | str | float | int
JsonLike: TypeAlias = JsonAtom | list["JsonLike"] | dict[str, "JsonLike"]
JsonDict: TypeAlias = dict[str, JsonLike]
