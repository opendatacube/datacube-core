from __future__ import absolute_import
import pytest
import yaml

from collections import namedtuple

from datacube.drivers import new_datasource, reader_drivers, writer_drivers
from datacube.drivers import index_drivers, index_driver_by_name
from datacube.drivers.indexes import IndexDriverCache
from datacube.storage.storage import RasterDatasetDataSource
from datacube.testutils import mk_sample_dataset
from datacube.model import MetadataType

S3_dataset = namedtuple('S3_dataset', ['macro_shape'])


def test_new_datasource_s3():
    pytest.importorskip('datacube.drivers.s3.storage.s3aio.s3lio')

    from datacube.drivers.s3 import driver as s3_driver
    from datacube.drivers.s3.datasource import S3DataSource

    bands = [dict(name='green',
                  path='')]
    dataset = mk_sample_dataset(bands, s3_driver.PROTOCOL + ':///foo', format=s3_driver.FORMAT)
    s3_dataset_fake = S3_dataset(macro_shape=(10, 12))
    dataset.s3_metadata = {'green': {'s3_dataset': s3_dataset_fake}}

    assert dataset.format == s3_driver.FORMAT
    assert dataset.uri_scheme == s3_driver.PROTOCOL

    rdr = new_datasource(dataset, 'green')
    assert rdr is not None
    assert isinstance(rdr, S3DataSource)


def test_new_datasource_fallback():
    bands = [dict(name='green',
                  path='')]
    dataset = mk_sample_dataset(bands, 'file:///foo', format='GeoTiff')

    assert dataset.uri_scheme == 'file'

    rdr = new_datasource(dataset, 'green')
    assert rdr is not None
    assert isinstance(rdr, RasterDatasetDataSource)


def test_reader_drivers():
    available_drivers = reader_drivers()
    assert isinstance(available_drivers, list)

    pytest.importorskip('datacube.drivers.s3.storage.s3aio.s3lio')
    assert 's3aio' in available_drivers


def test_writer_drivers():
    available_drivers = writer_drivers()
    assert 'netcdf' in available_drivers
    assert 'NetCDF CF' in available_drivers


def test_index_drivers():
    available_drivers = index_drivers()
    assert 'default' in available_drivers
    assert 's3aio_index' in available_drivers


def test_default_injection():
    cache = IndexDriverCache('datacube.plugins.index-no-such-prefix')
    assert cache.drivers() == ['default']


def test_netcdf_driver_import():
    try:
        import datacube.drivers.netcdf.driver
    except ImportError:
        assert False and 'Failed to load netcdf writer driver'


def test_metadata_type_from_doc():
    metadata_doc = yaml.safe_load('''
name: minimal
description: minimal metadata definition
dataset:
    id: [id]
    sources: [lineage, source_datasets]
    label: [label]
    creation_dt: [creation_dt]
    search_fields:
        some_custom_field:
            description: some custom field
            offset: [a,b,c,custom]
    ''')

    for name in index_drivers():
        driver = index_driver_by_name(name)
        metadata = driver.metadata_type_from_doc(metadata_doc)
        assert isinstance(metadata, MetadataType)
        assert metadata.id is None
        assert metadata.name == 'minimal'
        assert 'some_custom_field' in metadata.dataset_fields
