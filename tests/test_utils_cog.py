# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr
from dask.base import is_dask_collection
from dask.delayed import Delayed

from datacube.testutils import gen_tiff_dataset, mk_test_image, suppress_deprecations
from datacube.testutils.io import native_load, rio_slurp, rio_slurp_xarray
from datacube.utils.cog import _write_cog, to_cog, write_cog


def gen_test_data(prefix, dask=False, shape=None, dtype="int16", nodata=-999):
    w, h, ndw = 96, 64, 7
    if shape is not None:
        h, w = shape

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)

    ds, gbox = gen_tiff_dataset(
        SimpleNamespace(name="aa", values=aa, nodata=nodata), prefix
    )
    extras = {}

    if dask:
        extras.update(dask_chunks={"time": 1})

    xx = native_load(ds, **extras)

    return xx.aa.isel(time=0), ds


@pytest.mark.parametrize(
    "opts",
    [
        {},
        {"use_windowed_writes": True},
        {
            "intermediate_compression": {"compress": "deflate", "zlevel": 1},
            "use_windowed_writes": True,
        },
        {"intermediate_compression": True},
        {"intermediate_compression": "deflate"},
    ],
)
def test_cog_file(tmpdir, opts) -> None:
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp)

    with suppress_deprecations():
        # write to file
        ff = write_cog(  # Coverage test of deprecated function.
            xx, pp / "cog.tif", **opts
        )
    assert isinstance(ff, Path)
    assert ff == pp / "cog.tif"
    assert ff.exists()

    yy = rio_slurp_xarray(pp / "cog.tif")
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata

    with suppress_deprecations():
        _write_cog(  # Test of deprecated function
            np.stack([xx.values, xx.values]),
            xx.odc.geobox,
            pp / "cog-2-bands.tif",
            overview_levels=[],
            **opts,
        )

    yy, mm = rio_slurp(pp / "cog-2-bands.tif")
    assert mm.geobox == xx.odc.geobox
    assert yy.shape == (2, *xx.shape)
    np.testing.assert_array_equal(yy[0], xx.values)
    np.testing.assert_array_equal(yy[1], xx.values)

    with (
        pytest.raises(ValueError, match="Need 2d or 3d ndarray on input"),
        suppress_deprecations(),
    ):
        _write_cog(
            xx.values.ravel(), xx.odc.geobox, pp / "wontwrite.tif"
        )  # Test of deprecated function

    # sizes that are not multiples of 16
    # also check that supplying `nodata=` doesn't break things
    xx_odd = xx[:23, :63]
    with suppress_deprecations():
        ff = write_cog(  # Coverage test of deprecated function
            xx_odd, pp / "cog_odd.tif", nodata=xx_odd.attrs["nodata"], **opts
        )
    assert isinstance(ff, Path)
    assert ff == pp / "cog_odd.tif"
    assert ff.exists()

    yy = rio_slurp_xarray(pp / "cog_odd.tif")
    np.testing.assert_array_equal(yy.values, xx_odd.values)
    assert yy.odc.geobox == xx_odd.odc.geobox
    assert yy.nodata == xx_odd.nodata

    with suppress_deprecations(), pytest.warns(UserWarning):
        write_cog(
            xx, pp / "cog_badblocksize.tif", blocksize=50
        )  # Test of deprecated method

    # check writing floating point COG with no explicit nodata
    zz, ds = gen_test_data(pp, dtype="float32", nodata=None)
    # write to file
    with suppress_deprecations():
        ff = write_cog(zz, pp / "cog_float.tif", **opts)
    assert isinstance(ff, Path)
    assert ff == pp / "cog_float.tif"
    assert ff.exists()
    aa = rio_slurp_xarray(pp / "cog_float.tif")
    assert aa.attrs["nodata"] == "nan" or math.isnan(aa.attrs["nodata"])


def test_cog_file_dask(tmpdir) -> None:
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp, dask=True)
    assert is_dask_collection(xx)

    path = pp / "cog.tif"
    with suppress_deprecations():
        ff = write_cog(xx, path, overview_levels=[2, 4])  # Test of deprecated method
        assert isinstance(ff, Delayed)
        assert path.exists() is False
        assert ff.compute() == path
    assert path.exists()

    yy = rio_slurp_xarray(pp / "cog.tif")
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata


@pytest.mark.parametrize("shape", [None, (1024, 512)])
def test_cog_mem(tmpdir, shape) -> None:
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp, shape=shape)

    # write to memory 1
    with suppress_deprecations():
        bb = write_cog(xx, ":mem:")  # Test of deprecated function
    assert isinstance(bb, bytes)
    path = pp / "cog1.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata

    # write to memory 2
    with suppress_deprecations():
        bb = to_cog(xx)  # Test of deprecated function
    assert isinstance(bb, bytes)
    path = pp / "cog2.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata

    # write to memory 3 -- no overviews
    with suppress_deprecations():
        bb = to_cog(xx, overview_levels=[])  # Test of deprecated function
    assert isinstance(bb, bytes)
    path = pp / "cog3.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata


def test_cog_mem_dask(tmpdir) -> None:
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp, dask=True)

    # write to memory 1
    with suppress_deprecations():
        bb = write_cog(xx, ":mem:")  # Test of deprecated method
        assert isinstance(bb, Delayed)
        bb = bb.compute()
        assert isinstance(bb, bytes)

    path = pp / "cog1.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata

    # write to memory 2
    with suppress_deprecations():
        bb = to_cog(xx)  # Test of deprecated function
        assert isinstance(bb, Delayed)
        bb = bb.compute()
        assert isinstance(bb, bytes)
    path = pp / "cog2.tiff"
    with open(str(path), "wb") as f:
        f.write(bb)

    yy = rio_slurp_xarray(path)
    np.testing.assert_array_equal(yy.values, xx.values)
    assert yy.odc.geobox == xx.odc.geobox
    assert yy.nodata == xx.nodata


@pytest.mark.parametrize("use_windowed_writes", [False, True])
def test_cog_rgba(tmpdir, use_windowed_writes) -> None:
    pp = Path(str(tmpdir))
    xx, ds = gen_test_data(pp)
    pix = np.dstack([xx.values] * 4)
    rgba = xr.DataArray(pix, attrs=xx.attrs, dims=("y", "x", "band"), coords=xx.coords)
    assert rgba.odc.geobox == xx.odc.geobox
    assert rgba.shape[:2] == rgba.odc.geobox.shape

    with suppress_deprecations():
        ff = write_cog(
            rgba, pp / "cog.tif", use_windowed_writes=use_windowed_writes
        )  # Test of deprecated function
    yy = rio_slurp_xarray(ff)

    assert yy.odc.geobox == rgba.odc.geobox
    assert yy.shape == rgba.shape
    np.testing.assert_array_equal(yy.values, rgba.values)

    with pytest.raises(ValueError), suppress_deprecations():
        _write_cog(  # Test of deprecated function
            rgba.values[1:, :, :],
            rgba.odc.geobox,
            ":mem:",
            use_windowed_writes=use_windowed_writes,
        )
