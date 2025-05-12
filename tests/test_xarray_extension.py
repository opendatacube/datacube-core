# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest
import xarray as xr

from datacube.testutils import mk_sample_xr_dataset, remove_crs
from datacube.testutils.geom import epsg3857, epsg4326
from datacube.utils.xarray_geoextensions import _xarray_affine

multi_coords = xr.DataArray(
    np.zeros(1),
    [
        (
            "spec",
            pd.MultiIndex.from_arrays(
                np.array([["2001-01-01"], ["2001-01-01"]]), names=("time", "solar_day")
            ),
        )
    ],
).coords
single_coord = {"time": np.array(["2001-01-01"])}


@pytest.mark.parametrize(
    "odc_style_xr_dataset", [single_coord, multi_coords], indirect=True
)
def test_xr_extension(odc_style_xr_dataset) -> None:
    xx = odc_style_xr_dataset

    assert (1,) + xx.odc.geobox.shape == xx.B10.shape

    (sx, zz0, tx, zz1, sy, ty) = xx.affine[:6]
    assert (zz0, zz1) == (0, 0)

    xx = remove_crs(xx)

    # affine should still be valid
    A = _xarray_affine(xx)
    assert A is not None
    npt.assert_allclose(A * (0.5, 0.5), (xx.longitude.data[0], xx.latitude.data[0]))
    npt.assert_allclose(A * (0.5, 1.5), (xx.longitude.data[0], xx.latitude.data[1]))


def test_xr_geobox() -> None:
    xy = (10, 111)
    rxy = (10, -100)
    resolution = rxy[::-1]

    ds = mk_sample_xr_dataset(crs=epsg3857, xy=xy, resolution=resolution)

    assert ds.odc.geobox.crs == epsg3857
    assert ds.band.odc.geobox.crs == epsg3857
    assert ds.band.affine * (0, 0) == xy
    assert ds.band.affine * (1, 1) == tuple(a + b for a, b in zip(xy, rxy))

    assert ds.band[:, :2, :2].affine * (0, 0) == xy
    assert ds.band[:, :2, :2].affine * (1, 1) == tuple(a + b for a, b in zip(xy, rxy))
    assert ds.band.isel(time=0, x=0).affine is None

    xx = ds.band + 1000
    assert xx.odc.geobox is not None
    assert xx.odc.geobox == ds.band.odc.geobox

    assert mk_sample_xr_dataset(crs=epsg4326).odc.geobox.crs == epsg4326
    assert mk_sample_xr_dataset(crs=epsg4326).band.odc.geobox.crs == epsg4326

    assert mk_sample_xr_dataset(crs=None).affine is not None
