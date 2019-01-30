""" Tests for new RIO reader driver
"""
import pytest
from concurrent.futures import ThreadPoolExecutor

from datacube.testutils import mk_sample_dataset
from datacube.drivers.rio._reader import RDEntry
from datacube.storage import BandInfo

NetCDF = 'NetCDF'    # pylint: disable=invalid-name
GeoTIFF = 'GeoTIFF'  # pylint: disable=invalid-name


def test_rio_rd_entry():
    rde = RDEntry()

    assert 'file' in rde.protocols
    assert 's3' in rde.protocols

    assert GeoTIFF in rde.formats
    assert NetCDF in rde.formats

    assert rde.supports('file', NetCDF) is True
    assert rde.supports('s3', NetCDF) is False

    assert rde.supports('file', GeoTIFF) is True
    assert rde.supports('s3', GeoTIFF) is True

    assert rde.new_instance({}) is not None
    assert rde.new_instance({'max_workers': 2}) is not None

    with pytest.raises(ValueError):
        rde.new_instance({'pool': []})

    # check pool re-use
    pool = ThreadPoolExecutor(max_workers=1)
    rdr = rde.new_instance({'pool': pool})
    assert rdr._pool is pool


def test_rio_driver():
    nosuch_uri = 'file:///this-file-hopefully/doesnot/exist-4718193.tiff'
    rde = RDEntry()
    rdr = rde.new_instance({})

    assert rdr is not None

    load_ctx = rdr.new_load_context(None)
    load_ctx = rdr.new_load_context(load_ctx)

    bands = [dict(name='green',
                  path='')]
    ds = mk_sample_dataset(bands, nosuch_uri, format=GeoTIFF)
    assert ds.uri_scheme == 'file'

    bi = BandInfo(ds, 'green')
    assert bi.uri == nosuch_uri

    fut = rdr.open(bi, load_ctx)

    with pytest.raises(IOError):
        fut.result()
