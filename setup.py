#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


setup(name='datacube',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      packages=find_packages(
          exclude=('tests', 'tests.*', 'examples',
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
          'rasterio>=0.28',
          'singledispatch',
          'netcdf4',
          'pypeg2',
          'psycopg2',
          'gdal>=1.9',
          'dask',
          'setuptools',
          'toolz',
          'xarray',
          'scipy',
          'matplotlib',
          'numexpr',
          'future',
      ],
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
      long_description=open('README.md', 'r').read(),
      license='Apache License 2.0',
      entry_points={
          'console_scripts': [
              'datacube-search = datacube.scripts.search_tool:cli',
              'datacube = datacube.scripts.cli_app:cli'
          ]
      },
      )

