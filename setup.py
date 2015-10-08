#!/usr/bin/env python

from setuptools import setup

setup(
    name='datacube-experiments',
    description='Experimental Datacube v2 Ingestor',
    version='0.0.1',
    packages=['ingestor'],
    url='http://github.com/omad/datacube-experiments',
    install_requires=[
        'click',
        'eodatasets',
        'gdal',
        'pathlib',
        'pyyaml'
    ],
    entry_points='''
        [console_scripts]
        datacube_ingest=ingestor.ingest_from_yaml:main
    ''',
)
