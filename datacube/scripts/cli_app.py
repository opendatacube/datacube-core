# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube command-line interface
"""

import datacube.scripts.dataset
import datacube.scripts.metadata
import datacube.scripts.product
import datacube.scripts.spindex
import datacube.scripts.system
import datacube.scripts.user  # noqa: F401
from datacube.ui.click import cli

if __name__ == "__main__":
    cli()
