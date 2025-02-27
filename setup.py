#!/usr/bin/env python

from setuptools import setup, find_packages

tests_require = [
    'hypothesis',
    'pycodestyle',
    'pylint',
    'pytest',
    'pytest-cov',
    'pytest-timeout',
    'pytest-httpserver',
    'moto<5.0',  # 5.0 will require changes to some tests.
    'psycopg2',
    'netcdf4'
]

types_require = [
    "mypy<1.14",
    "types-affine",
    "types-cachetools",
    "types-jsonschema",
    "types-psutil",
    "types-python-dateutil",
    "types-PyYAML",
    "types-redis",
    "types-setuptools",
    "boto3-stubs",
    "botocore-stubs"
]

doc_require = [
    'Sphinx',
    'sphinx_autodoc_typehints',  # Propagate mypy info into docs
    'sphinx-click',
    'recommonmark',
    'autodocsumm',
    'ipython',
    'beautifulsoup4',
    'nbsphinx',
    'ipython',
    'pydata-sphinx-theme',
    "sphinx-design",
]

extras_require = {
    'performance': ['ciso8601', 'bottleneck'],
    'distributed': ['distributed', 'dask[distributed]'],
    'doc': doc_require,
    's3': ['boto3', 'botocore'],
    'test': tests_require,
    "types": types_require,
    'cf': ['compliance-checker>=4.0.0'],
    'netcdf': ['netcdf4'],
    'postgres': ['psycopg2'],
}

extras_require['dev'] = sorted(set(sum([extras_require[k] for k in [
    'test',
    'types',
    'doc',
    'performance',
    's3',
    'distributed',
    'netcdf',
    'postgres',
]], [])))

# An 'all' option, following ipython naming conventions.
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

extra_plugins = dict(read=[], write=[], index=[])

setup(
    name='datacube',
    python_requires='>=3.10.0',

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
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],

    packages=find_packages(
        exclude=('tests', 'tests.*',
                 'integration_tests', 'integration_tests.*')
    ),
    package_data={
        '': ['*.yaml', '*/*.yaml'],
        'datacube': ['py.typed'],
        'datacube.drivers.postgis': ['alembic.ini', 'alembic/*.py*', 'alembic/README', 'alembic/versions/*.py'],
    },
    scripts=[],
    install_requires=[
        'affine',
        'antimeridian',
        'attrs>=18.1',
        'pyproj>=2.5',
        'shapely>=2.0',
        'cachetools',
        'click>=5.0',
        'cloudpickle>=0.4',
        'dask[array]',
        'distributed',
        'jsonschema>=4.18',  # New reference resolution API
        'numpy>=1.26.0',
        'lark',
        'pandas',
        'python-dateutil',
        'pyyaml',
        'rasterio>=1.3.11',  # Deleted Numpy version in 1.3.10
        'ruamel.yaml',
        'sqlalchemy>=2.0',  # GeoAlchemy2 requires >=1.4.
        'GeoAlchemy2',
        "alembic",
        'toolz',
        'xarray>=0.9',  # >0.9 fixes most problems with `crs` attributes being lost
        'packaging',
        'odc-geo>=0.4.8',
        'deprecat',
    ],
    extras_require=extras_require,
    tests_require=tests_require,

    entry_points={
        'console_scripts': [
            'datacube = datacube.scripts.cli_app:cli',
            'datacube-search = datacube.scripts.search_tool:cli',
            'datacube-worker = datacube.execution.worker:main',
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
            'postgres = datacube.index.postgres.index:index_driver_init',
            'null = datacube.index.null.index:index_driver_init',
            'memory = datacube.index.memory.index:index_driver_init',
            'postgis = datacube.index.postgis.index:index_driver_init',
            *extra_plugins['index'],
        ],
    },
)
