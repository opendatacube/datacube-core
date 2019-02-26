""" Test New IO driver loading
"""

from datacube.storage._load import (
    xr_load,
)

from datacube.api.core import Datacube
from datacube.testutils import mk_sample_dataset
from datacube.testutils.io import rio_slurp
from datacube.testutils.iodriver import mk_rio_driver, tee_new_load_context


def test_new_xr_load(data_folder):
    base = "file://" + str(data_folder) + "/metadata.yml"

    rdr = mk_rio_driver()
    assert rdr is not None

    _bands = []

    def band_info_collector(bands, ctx):
        for b in bands:
            _bands.append(b)

    tee_new_load_context(rdr, band_info_collector)

    band = dict(name='a',
                path='test.tif')
    ds = mk_sample_dataset([band], base)

    sources = Datacube.group_datasets([ds], 'time')

    im, meta = rio_slurp(str(data_folder) + '/test.tif')
    measurements = [ds.type.measurements['a']]

    xx, _ = xr_load(sources, meta.gbox, measurements, rdr)

    assert len(_bands) == 1

    assert im[0].shape == xx.a.isel(time=0).shape
    # TODO: verify pixel values
