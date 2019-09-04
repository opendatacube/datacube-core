from pathlib import Path
import numpy as np
from types import SimpleNamespace

from datacube.testutils import (
    mk_test_image,
    gen_tiff_dataset,
)
from datacube.testutils.io import native_load, rio_slurp_xarray
from datacube.utils.cog import write_cog, to_cog


def gen_test_data(prefix, dask=False):
    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)

    ds, gbox = gen_tiff_dataset(
        SimpleNamespace(name='aa', values=aa, nodata=nodata), prefix)
    extras = {}

    if dask:
        extras.update(dask_chunks={})

    xx = native_load(ds, **extras)

    return xx.aa.isel(time=0), ds


def test_cog_file(tmpdir):
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp)

    # write to file
    ff = write_cog(xx, pp / "cog.tif")
    assert isinstance(ff, Path)
    assert ff == pp / "cog.tif"
    assert ff.exists()

    yy = rio_slurp_xarray(pp / "cog.tif")
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.geobox == xx.geobox
    assert yy.nodata == xx.nodata


def test_cog_mem(tmpdir):
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp)

    # write to memory 1
    bb = write_cog(xx, ":mem:")
    assert isinstance(bb, bytes)
    path = pp / "cog1.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.geobox == xx.geobox
    assert yy.nodata == xx.nodata

    # write to memory 2
    bb = to_cog(xx)
    assert isinstance(bb, bytes)
    path = pp / "cog2.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.geobox == xx.geobox
    assert yy.nodata == xx.nodata
