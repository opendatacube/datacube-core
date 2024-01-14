# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Add geometric extensions to :class:`xarray.Dataset` and :class:`xarray.DataArray` for use
with Data Cube by Monkey Patching those classes.

This extension is reliant on an `xarray` object having a `.crs` property of type
:class:`odc.geo.crs.CRS`. This is used to inspect the spatial dimensions of the
:class:`Dataset` or :class:`DataArray`, and provide new attributes for accessing a
:class:`odc.geo.geobox.GeoBox`, affine transform and extent for the dataset as
`.geobox`, `.affine` and `.extent` respectively.

"""
import warnings
import xarray
from odc.geo import resxy_
from odc.geo.math import affine_from_axis
from odc.geo.xr import spatial_dims
from odc.geo._xr_interop import _xarray_geobox as _xr_geobox


def _xarray_affine_impl(obj):
    sdims = spatial_dims(obj, relaxed=True)
    if sdims is None:
        return None, None

    yy, xx = (obj[dim] for dim in sdims)
    fallback_res = (coord.attrs.get('resolution', None) for coord in (xx, yy))
    res_x, res_y = next(fallback_res), next(fallback_res)
    fallback_res = None if (res_x, res_y) == (None, None) else resxy_(res_x, res_y)
    return affine_from_axis(xx.values, yy.values, fallback_res), sdims


def _xarray_affine(obj):
    transform, _ = _xarray_affine_impl(obj)
    return transform


def _xarray_extent(obj):
    geobox = obj.odc.geobox
    return None if geobox is None else geobox.extent


def _xarray_geobox(obj):
    warnings.warn(
        'Geobox extraction logic has moved to odc-geo and the .geobox property is now deprecated.'
        'Please access via .odc.geobox instead.',
        DeprecationWarning,
        stacklevel=2)
    return _xr_geobox(obj)


xarray.Dataset.geobox = property(_xarray_geobox)    # type: ignore
xarray.Dataset.affine = property(_xarray_affine)    # type: ignore
xarray.Dataset.extent = property(_xarray_extent)    # type: ignore
xarray.DataArray.geobox = property(_xarray_geobox)  # type: ignore
xarray.DataArray.affine = property(_xarray_affine)  # type: ignore
xarray.DataArray.extent = property(_xarray_extent)  # type: ignore
