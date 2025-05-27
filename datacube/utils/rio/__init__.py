# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
This will move into IO driver eventually.

For now this provides tools to configure GDAL environment for performant reads from S3.
"""

from ._rio import (
    activate_from_config,
    activate_rio_env,
    configure_s3_access,
    deactivate_rio_env,
    get_rio_env,
    set_default_rio_config,
)

__all__ = [
    "activate_from_config",
    "activate_rio_env",
    "configure_s3_access",
    "deactivate_rio_env",
    "get_rio_env",
    "set_default_rio_config",
]
