#!/usr/bin/env python

import versioneer
from setuptools import setup, find_packages

extras_require = {
    'performance': ['ciso8601', 'bottleneck'],
    'interactive': ['matplotlib'],
    'distributed': ['distributed', 'dask[distributed]'],
    'analytics': ['scipy', 'pyparsing', 'numexpr'],
    'doc': ['Sphinx'],
    'test': ['pytest', 'pytest-cov', 'mock', 'pep8', 'pylint'],
}
# An 'all' option, following ipython naming conventions.
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

long_description = """Data Cube provides an integrated gridded data analysis environment
for earth observation satellite and related data from multiple satellite and other acquisition systems"""

setup(name='datacube',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
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
          'future',
      ],
      extras_require=extras_require,
      tests_require=[
          'pytest',
          'pytest-cov',
          'mock'
      ],
      url='https://github.com/data-cube/agdc-v2',
      author='AGDC Collaboration',
      maintainer='AGDC Collaboration',
      maintainer_email='',
      description='AGDC v2',
      long_description=long_description,
      license='Apache License 2.0',
      entry_points={
          'console_scripts': [
              'datacube-search = datacube.scripts.search_tool:cli',
              'datacube = datacube.scripts.cli_app:cli',
              'pixeldrill = datacube_apps.pixeldrill:main [interactive]',
              'movie_generator = datacube_apps.movie_generator:main'
          ]
      },
      )
