from __future__ import absolute_import
#!/usr/bin/env python

from distutils.core import setup

version = '0.0.0'

setup(name='agdc-v2',
      version=version,
      packages=[
          'gdf',
          'analytics',
          'analytics_utils',
          'execution_engine',
      ],
      package_data={
          'gdf': ['gdf_default.conf']
      },
      scripts=[
      ],
      requires=[
          'psycopg2',
          'gdal',
          'numexpr',
          'numpy',
          'matplotlib',
          'netcdf4',
          'scipy',
          'pytz'
      ],
      url='https://github.com/data-cube/agdc-v2',
      author='AGDC Collaboration',
      maintainer='AGDC Collaboration',
      maintainer_email='',
      description='AGDC v2',
      long_description='Australian Geoscience Data Cube v2',
      license='Apache License 2.0'
      )
