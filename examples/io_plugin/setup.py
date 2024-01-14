# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from setuptools import setup, find_packages

setup(
    name='dcio_example',
    version='1.1',
    description='Test IO plugins for datacube',
    author='Open Data Cube',
    packages=find_packages(),

    entry_points={
        'datacube.plugins.io.read': [
            'pickle=dcio_example.pickles:rdr_driver_init',
            'zeros=dcio_example.zeros:init_driver',
            'xarray_3d=dcio_example.xarray_3d:reader_driver_init',
        ],
        'datacube.plugins.io.write': [
            'pickle=dcio_example.pickles:writer_driver_init',
        ],
    },
)
