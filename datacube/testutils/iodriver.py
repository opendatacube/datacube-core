""" Reader driver construction for tests
"""
from pathlib import Path
from datacube.testutils import mk_sample_dataset
from datacube.drivers.rio._reader import (
    RDEntry,
)
from datacube.storage import BandInfo
from datacube.testutils.threads import FakeThreadPoolExecutor

NetCDF = 'NetCDF'    # pylint: disable=invalid-name
GeoTIFF = 'GeoTIFF'  # pylint: disable=invalid-name


def mk_rio_driver():
    pool = FakeThreadPoolExecutor()
    rde = RDEntry()
    return rde.new_instance({'pool': pool,
                             'allow_custom_pool': True})


def mk_band(name: str,
            base_uri: str,
            path: str = '',
            format: str = GeoTIFF,  # pylint: disable=redefined-builtin
            **extras) -> BandInfo:
    """
    **extras**:
       layer, band, nodata, dtype, units, aliases
    """
    band_opts = {k: extras.pop(k)
                 for k in 'path layer band nodata dtype units aliases'.split() if k in extras}

    band = dict(name=name, path=path, **band_opts)
    ds = mk_sample_dataset([band], base_uri, format=format, **extras)
    return BandInfo(ds, name)


def open_reader(path: str,
                band_name: str = 'b1',
                format: str = GeoTIFF,  # pylint: disable=redefined-builtin
                **extras):
    """
    **extras**:
       layer, band, nodata, dtype, units, aliases
    """
    rdr = mk_rio_driver()
    base_uri = Path(path).absolute().as_uri()
    bi = mk_band(band_name, base_uri, format=format, **extras)
    load_ctx = rdr.new_load_context(iter([bi]), None)
    fut = rdr.open(bi, load_ctx)
    return fut.result()


def tee_new_load_context(rdr, new_impl):
    """ When calling rdr.new_load_context(bands, old_ctx) tee data to new_impl
    """
    _real_impl = rdr.new_load_context

    def patched(bands, old_ctx):
        bands = list(bands)
        new_impl(iter(bands), old_ctx)
        return _real_impl(iter(bands), old_ctx)

    rdr.new_load_context = patched
