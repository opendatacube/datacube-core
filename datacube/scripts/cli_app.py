#!/usr/bin/env python

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube command-line interface
"""


from datacube.ui.click import cli
import datacube.scripts.dataset
import datacube.scripts.ingest
import datacube.scripts.product
import datacube.scripts.metadata
import datacube.scripts.system
import datacube.scripts.user


if __name__ == '__main__':
    cli()
