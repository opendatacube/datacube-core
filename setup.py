#!/usr/bin/env python


from setuptools import setup, find_packages


setup(
    name='cube-data',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'future',
        'numpy',
        'rasterio'
    ],
    license='Apache License 2.0',
)
