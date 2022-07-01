#!/usr/bin/env python

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube command-line interface
"""


from datacube.ui.click import cli
import datacube.scripts.dataset   # noqa: F401
import datacube.scripts.ingest    # noqa: F401
import datacube.scripts.product   # noqa: F401
import datacube.scripts.metadata  # noqa: F401
import datacube.scripts.system    # noqa: F401
import datacube.scripts.user      # noqa: F401


if __name__ == '__main__':
    cli()
