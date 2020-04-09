#!/usr/bin/env python

from setuptools import setup, find_packages

tests_require = [
    'compliance-checker>=4.0.0',
    'hypothesis',
    'mock',
    'pycodestyle',
    'pylint',
    'pytest',
    'pytest-cov',
    'pytest-timeout',
    'pytest-httpserver',
    'moto',
]

extras_require = {
    'performance': ['ciso8601', 'bottleneck'],
    'interactive': ['matplotlib', 'fiona'],
    'distributed': ['distributed', 'dask[distributed]'],
    'doc': ['Sphinx', 'setuptools'],
    'replicas': ['paramiko', 'sshtunnel', 'tqdm'],
    'celery': ['celery>=4', 'redis'],
    's3': ['boto3'],
    'test': tests_require,
}
# An 'all' option, following ipython naming conventions.
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

extra_plugins = dict(read=[], write=[], index=[])

setup(
    name='datacube',
    python_requires='>=3.6.0',

    url='https://github.com/opendatacube/datacube-core',
    author='Open Data Cube',
    maintainer='Open Data Cube',
    maintainer_email='',
    description='An analysis environment for satellite and other earth observation data',
    long_description=open('README.rst').read(),
    long_description_content_type='text/x-rst',
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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
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
        'datacube_apps/scripts/pbs_helpers.sh'
    ],
    install_requires=[
        'affine',
        'pyproj>=2.5',
        'shapely>=1.6.4',
        'cachetools',
        'click>=5.0',
        'cloudpickle>=0.4',
        'dask[array]',
        'distributed',
        'jsonschema',
        'netcdf4',
        'numpy',
        'psycopg2',
        'lark-parser>=0.6.7',
        'python-dateutil',
        'pyyaml',
        'rasterio>=1.0.2',  # Multi-band re-project fixed in that version
        'sqlalchemy',
        'toolz',
        'xarray>=0.9',  # >0.9 fixes most problems with `crs` attributes being lost
    ],
    extras_require=extras_require,
    tests_require=tests_require,

    entry_points={
        'console_scripts': [
            'datacube = datacube.scripts.cli_app:cli',
            'datacube-search = datacube.scripts.search_tool:cli',
            'datacube-stacker = datacube_apps.stacker:main',
            'datacube-worker = datacube.execution.worker:main',
            'datacube-fixer = datacube_apps.stacker:fixer_main',
            'datacube-ncml = datacube_apps.ncml:ncml_app',
            'pixeldrill = datacube_apps.pixeldrill:main [interactive]',
            'movie_generator = datacube_apps.movie_generator:main',
            'datacube-simple-replica = datacube_apps.simple_replica:replicate [replicas]'
        ],
        'datacube.plugins.io.read': [
            'netcdf = datacube.drivers.netcdf.driver:reader_driver_init',
            *extra_plugins['read'],
        ],
        'datacube.plugins.io.write': [
            'netcdf = datacube.drivers.netcdf.driver:writer_driver_init',
            *extra_plugins['write'],
        ],
        'datacube.plugins.index': [
            'default = datacube.index.index:index_driver_init',
            *extra_plugins['index'],
        ],
    },
)
