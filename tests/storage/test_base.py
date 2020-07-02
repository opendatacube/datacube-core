import pytest
from datacube.storage import BandInfo
from datacube.testutils import mk_sample_dataset
from datacube.storage._base import _get_band_and_layer


def test_band_layer():
    def t(band=None, layer=None):
        return _get_band_and_layer(dict(band=band, layer=layer))

    assert t() == (None, None)
    assert t(1) == (1, None)
    assert t(None, 3) == (3, None)
    assert t(1, 'foo') == (1, 'foo')
    assert t(None, 'foo') == (None, 'foo')

    bad_inputs = [('string', None),  # band has to be int|None
                  (None, {}),  # layer has to be int|str|None
                  (1, 3)]  # if band is set layer should be str|None

    for bad in bad_inputs:
        with pytest.raises(ValueError):
            t(*bad)


def test_band_info():
    bands = [dict(name=n,
                  dtype='uint8',
                  units='K',
                  nodata=33,
                  path=n+'.tiff')
             for n in 'a b c'.split(' ')]

    ds = mk_sample_dataset(bands,
                           uri='file:///tmp/datataset.yml',
                           format='GeoTIFF')

    binfo = BandInfo(ds, 'b')
    assert binfo.name == 'b'
    assert binfo.band is None
    assert binfo.layer is None
    assert binfo.dtype == 'uint8'
    assert binfo.transform is None
    assert binfo.crs is None
    assert binfo.units == 'K'
    assert binfo.nodata == 33
    assert binfo.uri == 'file:///tmp/b.tiff'
    assert binfo.format == ds.format
    assert binfo.driver_data is None
    assert binfo.uri_scheme == 'file'

    with pytest.raises(ValueError):
        BandInfo(ds, 'no_such_band')

    # Check case where dataset is missing band that is present in the product
    del ds.metadata_doc['image']['bands']['c']
    with pytest.raises(ValueError):
        BandInfo(ds, 'c')

    ds.uris = []
    with pytest.raises(ValueError):
        BandInfo(ds, 'a')

    ds.uris = None
    with pytest.raises(ValueError):
        BandInfo(ds, 'a')
