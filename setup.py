#!/usr/bin/env python

from setuptools import setup

setup(
    name='datacube-experiments',
    description='Experimental Datacube v2 Ingestor',
    version='0.0.1',
    packages=['ingester'],
    url='http://github.com/omad/datacube-experiments',
    install_requires=[
        'click',
        'eodatasets',
        'eotools',
        'gdal',
        'pathlib',
        'pyyaml',
        'numpy',
        'netCDF4',
    ],
    tests_require=[
        'pytest',
    ],

    entry_points={
      'console_scripts': [
          'datacube_ingest = ingester.datacube_ingestor:main',
          'print_image = ingester.utils:print_image',
      ]
    },
)
