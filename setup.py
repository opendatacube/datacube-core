#!/usr/bin/env python


from distutils.core import setup


setup(
    name='cube-data',
    version='0.1',
    packages=['cubeaccess'],
    requires=[
        'future',
        'numpy',
    ],
    license='Apache License 2.0',
)
