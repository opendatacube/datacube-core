""" Test New IO driver loading
"""

from datacube.storage._load import (
    xr_load,
)

from datacube.api.core import Datacube
from datacube.drivers.rio._reader import RDEntry
from datacube.testutils import mk_sample_dataset
from datacube.testutils.io import rio_slurp
from datacube.testutils.threads import FakeThreadPoolExecutor


def mk_reader():
    pool = FakeThreadPoolExecutor()
    rde = RDEntry()
    return rde.new_instance({'pool': pool,
                             'allow_custom_pool': True})


def test_new_xr_load(data_folder):
    base = "file://" + str(data_folder) + "/metadata.yml"

    rdr = mk_reader()
    assert rdr is not None

    band = dict(name='a',
                path='test.tif')
    ds = mk_sample_dataset([band], base)

    sources = Datacube.group_datasets([ds], 'time')

    im, meta = rio_slurp(str(data_folder) + '/test.tif')
    measurements = [ds.type.measurements['a']]

    xx, _ = xr_load(sources, meta.gbox, measurements, rdr)

    assert im[0].shape == xx.a.isel(time=0).shape
    # TODO: verify pixel values
