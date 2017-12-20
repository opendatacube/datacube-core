from __future__ import absolute_import
from collections import namedtuple
from .util import mk_sample_dataset
from datacube.drivers import new_datasource
from datacube.drivers.s3 import driver as s3_driver
from datacube.drivers.s3.datasource import S3DataSource
from datacube.storage.storage import RasterDatasetSource

S3_dataset = namedtuple('S3_dataset', ['macro_shape'])


def test_new_datasource_s3():
    bands = [dict(name='green',
                  path='')]
    dataset = mk_sample_dataset(bands, 's3+block:///foo', format='s3block')
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
    assert isinstance(rdr, RasterDatasetSource)
