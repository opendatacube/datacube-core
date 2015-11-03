from __future__ import absolute_import
#!/usr/bin/env python

from setuptools import setup

version = '0.0.0'

setup(name='agdc-v2',
      version=version,
      packages=[
          'datacube.gdf',
          'datacube.analytics',
          'datacube.analytics_utils',
          'datacube.execution_engine',
          'datacube.ingester'
      ],
      package_data={
          'datacube/gdf': ['gdf_default.conf']
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
          'pytz',
      ],
      install_requires=[
          'click',
          'pathlib',
          'pyyaml',
      ],
      tests_require=[
          'pytest',
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
              'datacube_ingester = datacube.ingester.ingester:main'
          ]
      },
)
