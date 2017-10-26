"""
Add geometric extensions to :class:`xarray.Dataset` and :class:`xarray.DataArray` for use
with Data Cube by Monkey Patching those classes.

This extension is reliant on an `xarray` object having a `.crs` property of type
:class:`datacube.utils.geometry.CRS`. This is used to inspect the spatial dimensions of the
:class:`Dataset` or :class:`DataArray`, and provide new attributes for accessing a
:class:`datacube.utils.geometry.GeoBox`, affine transform and extent for the dataset as
`.geobox`, `.affine` and `.extent` respectively.

"""
from __future__ import absolute_import
from affine import Affine
import xarray

from datacube.utils import data_resolution_and_offset, geometry


def _xarray_affine(obj):
    dims = obj.crs.dimensions
    xres, xoff = data_resolution_and_offset(obj[dims[1]].values)
    yres, yoff = data_resolution_and_offset(obj[dims[0]].values)
    return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)


def _xarray_extent(obj):
    return obj.geobox.extent


def _xarray_geobox(obj):
    dims = obj.crs.dimensions
    return geometry.GeoBox(obj[dims[1]].size, obj[dims[0]].size, obj.affine, obj.crs)


xarray.Dataset.geobox = property(_xarray_geobox)
xarray.Dataset.affine = property(_xarray_affine)
xarray.Dataset.extent = property(_xarray_extent)
xarray.DataArray.geobox = property(_xarray_geobox)
xarray.DataArray.affine = property(_xarray_affine)
xarray.DataArray.extent = property(_xarray_extent)
