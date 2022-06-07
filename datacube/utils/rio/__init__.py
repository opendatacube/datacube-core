# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
This will move into IO driver eventually.

For now this provides tools to configure GDAL environment for performant reads from S3.
"""
from ._rio import (
    activate_rio_env,
    deactivate_rio_env,
    get_rio_env,
    set_default_rio_config,
    activate_from_config,
    configure_s3_access,
)

__all__ = (
    'activate_rio_env',
    'deactivate_rio_env',
    'get_rio_env',
    'set_default_rio_config',
    'activate_from_config',
    'configure_s3_access',
)
