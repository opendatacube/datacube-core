""" Test New IO driver loading
"""

import numpy as np

from datacube.storage._load import (
    xr_load, _default_fuser
)

from datacube.api.core import Datacube
from datacube.testutils import mk_sample_dataset
from datacube.testutils.io import rio_slurp
from datacube.testutils.iodriver import mk_rio_driver, tee_new_load_context


def test_default_fuser():
    dest = np.full((2, 2), -1.0)
    src1 = np.array([[0.0, -1.0], [-1.0, 6.0]])

    _default_fuser(dest, src1, -1.0)
    assert np.all(dest == src1)

    src2 = np.array([[9.0, np.nan], [-1.0, 3.0]])
    _default_fuser(dest, src2, -1.0)
    assert np.allclose(dest, np.array([[0.0, np.nan], [-1.0, 6.0]]), equal_nan=True)

    src3 = np.full((2, 2), 55.0)
    _default_fuser(dest, src3, -1.0)
    assert np.all(dest == np.array([[0.0, 55.0], [55.0, 6.0]]))

    dest = np.full((2, 2), -1)
    src1 = np.array([[0, -1], [-1, 6]])

    _default_fuser(dest, src1, -1)
    assert np.all(dest == src1)

    src2 = np.array([[9, 4], [-1, 3]])
    _default_fuser(dest, src1, None)
    assert np.all(dest == src1)


def test_new_xr_load(data_folder):
    base = "file://" + str(data_folder) + "/metadata.yml"

    rdr = mk_rio_driver()
    assert rdr is not None

    _bands = []

    def band_info_collector(bands, ctx):
        for b in bands:
            _bands.append(b)

    tee_new_load_context(rdr, band_info_collector)

    band_a = dict(name='a',
                  path='test.tif')

    band_b = dict(name='b',
                  band=2,
                  path='test.tif')

    ds = mk_sample_dataset([band_a, band_b], base)

    sources = Datacube.group_datasets([ds], 'time')

    im, meta = rio_slurp(str(data_folder) + '/test.tif')
    measurements = [ds.type.measurements[n] for n in ('a', 'b')]
    measurements[1]['fuser'] = lambda dst, src: _default_fuser(dst, src, measurements[1].nodata)

    xx, _ = xr_load(sources, meta.gbox, measurements, rdr)

    assert len(_bands) == 2

    assert im[0].shape == xx.a.isel(time=0).shape
    assert im[1].shape == xx.b.isel(time=0).shape

    np.testing.assert_array_equal(im[0], xx.a.values[0])
    np.testing.assert_array_equal(im[1], xx.b.values[0])
