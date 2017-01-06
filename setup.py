#!/usr/bin/env python

import versioneer
from setuptools import setup, find_packages

tests_require = ['pytest', 'pytest-cov', 'mock', 'pep8', 'pylint==1.6.4', 'hypothesis', 'compliance-checker']

extras_require = {
    'performance': ['ciso8601', 'bottleneck'],
    'interactive': ['matplotlib', 'fiona'],
    'distributed': ['distributed', 'dask[distributed]'],
    'analytics': ['scipy', 'pyparsing', 'numexpr'],
    'doc': ['Sphinx'],
    'test': tests_require,
}
# An 'all' option, following ipython naming conventions.
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

setup(
    name='datacube',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),

    url='https://github.com/data-cube/agdc-v2',
    author='AGDC Collaboration',
    maintainer='AGDC Collaboration',
    maintainer_email='',
    description='An analysis environment for satellite and other earth observation data',
    long_description=open('README.rst').read(),
    license='Apache License 2.0',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: BSD",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],

    packages=find_packages(
        exclude=('tests', 'tests.*',
                 'integration_tests', 'integration_tests.*')
    ),
    package_data={
        '': ['*.yaml', '*/*.yaml'],
    },
    scripts=[
    ],
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        'click>=5.0',
        'pathlib',
        'pyyaml',
        'sqlalchemy',
        'python-dateutil',
        'jsonschema',
        'cachetools',
        'numpy',
        'rasterio>=0.34',
        'singledispatch',
        'netcdf4',
        'pypeg2',
        'psycopg2',
        'gdal>=1.9',
        'dask[array]',
        'setuptools',
        'xarray',
    ],
    extras_require=extras_require,
    tests_require=tests_require,

    entry_points={
        'console_scripts': [
            'datacube-search = datacube.scripts.search_tool:cli',
            'datacube = datacube.scripts.cli_app:cli',
            'datacube-stacker = datacube_apps.stacker:main',
            'pixeldrill = datacube_apps.pixeldrill:main [interactive]',
            'movie_generator = datacube_apps.movie_generator:main'
        ]
    },
)
