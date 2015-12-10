#!/usr/bin/env python

from setuptools import setup, find_packages

version = '0.0.0'

setup(name='agdc-v2',
      version=version,
      packages=find_packages(
          exclude=('tests', 'tests.*', 'examples',
                   'integration_tests', 'integration_tests.*')
      ),
      package_data={
          'gdf_tests': ['gdf_default.conf'],
          '': ['*.yaml'],
      },
      scripts=[
      ],
      install_requires=[
          'click',
          'pathlib',
          'pyyaml',
          'sqlalchemy',
          'python-dateutil',
          'cachetools',
          'numpy',
          'rasterio',
          'netcdf4',
          'pypeg2',
          'psycopg2',
          'pytz',
          'gdal',
      ],
      tests_require=[
          'pytest',
          'mock'
      ],
      url='https://github.com/data-cube/agdc-v2',
      author='AGDC Collaboration',
      maintainer='AGDC Collaboration',
      maintainer_email='',
      description='AGDC v2',
      long_description='Australian Geoscience Data Cube v2',
      license='Apache License 2.0',
      entry_points={
          'console_scripts': [
              'datacube-ingest = datacube.scripts.run_ingest:cli',
              'datacube-config = datacube.scripts.config_tool:cli',
              'datacube-search = datacube.scripts.search_tool:cli'
          ]
      },
      )
