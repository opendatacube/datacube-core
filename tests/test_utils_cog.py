import pytest
from pathlib import Path
import numpy as np
import xarray as xr
from types import SimpleNamespace
from dask.delayed import Delayed
import dask

from datacube.testutils import (
    mk_test_image,
    gen_tiff_dataset,
    remove_crs,
)
from datacube.testutils.io import native_load, rio_slurp_xarray, rio_slurp
from datacube.utils.cog import write_cog, to_cog, _write_cog


def gen_test_data(prefix, dask=False):
    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)

    ds, gbox = gen_tiff_dataset(
        SimpleNamespace(name='aa', values=aa, nodata=nodata), prefix)
    extras = {}

    if dask:
        extras.update(dask_chunks={'time': 1})

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

    _write_cog(np.stack([xx.values, xx.values]),
               xx.geobox,
               pp / "cog-2-bands.tif",
               overview_levels=[])

    yy, mm = rio_slurp(pp / "cog-2-bands.tif")
    assert mm.gbox == xx.geobox
    assert yy.shape == (2, *xx.shape)
    np.testing.assert_array_equal(yy[0], xx.values)
    np.testing.assert_array_equal(yy[1], xx.values)

    with pytest.raises(ValueError, match="Need 2d or 3d ndarray on input"):
        _write_cog(xx.values.ravel(), xx.geobox, pp / "wontwrite.tif")


def test_cog_file_dask(tmpdir):
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp, dask=True)
    assert dask.is_dask_collection(xx)

    path = pp / "cog.tif"
    ff = write_cog(xx, path)
    assert isinstance(ff, Delayed)
    assert path.exists() is False
    assert ff.compute() == path
    assert path.exists()

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

    # write to memory 3 -- no overviews
    bb = to_cog(xx, overview_levels=[])
    assert isinstance(bb, bytes)
    path = pp / "cog3.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.geobox == xx.geobox
    assert yy.nodata == xx.nodata


def test_cog_mem_dask(tmpdir):
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp, dask=True)

    # write to memory 1
    bb = write_cog(xx, ":mem:")
    assert isinstance(bb, Delayed)
    bb = bb.compute()
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
    assert isinstance(bb, Delayed)
    bb = bb.compute()
    assert isinstance(bb, bytes)
    path = pp / "cog2.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.geobox == xx.geobox
    assert yy.nodata == xx.nodata


@pytest.mark.parametrize("with_dask", [True, False])
def test_cog_no_crs(tmpdir, with_dask):
    pp = Path(str(tmpdir))

    xx, ds = gen_test_data(pp, dask=with_dask)
    xx = remove_crs(xx)

    with pytest.raises(ValueError):
        write_cog(xx, ":mem:")

    with pytest.raises(ValueError):
        to_cog(xx)


def test_cog_rgba(tmpdir):
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp)
    pix = np.dstack([xx.values] * 4)
    rgba = xr.DataArray(pix,
                        attrs=xx.attrs,
                        dims=('y', 'x', 'band'),
                        coords=xx.coords)
    assert(rgba.geobox == xx.geobox)
    assert(rgba.shape[:2] == rgba.geobox.shape)

    ff = write_cog(rgba, pp / "cog.tif")
    yy = rio_slurp_xarray(ff)

    assert(yy.geobox == rgba.geobox)
    assert(yy.shape == rgba.shape)
    np.testing.assert_array_equal(yy.values, rgba.values)

    with pytest.raises(ValueError):
        _write_cog(rgba.values[1:, :, :], rgba.geobox, ':mem:')
