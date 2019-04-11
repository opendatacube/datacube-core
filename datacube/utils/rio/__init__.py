"""
This will move into IO driver eventually.

For now this provides tools to configure GDAL environment for performant reads from S3.
"""
from ._rio import (
    activate_rio_env,
    get_rio_env,
)

__all__ = (
    'activate_rio_env',
    'get_rio_env',
)
