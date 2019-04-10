"""
Add geometric extensions to :class:`xarray.Dataset` and :class:`xarray.DataArray` for use
with Data Cube by Monkey Patching those classes.

This extension is reliant on an `xarray` object having a `.crs` property of type
:class:`datacube.utils.geometry.CRS`. This is used to inspect the spatial dimensions of the
:class:`Dataset` or :class:`DataArray`, and provide new attributes for accessing a
:class:`datacube.utils.geometry.GeoBox`, affine transform and extent for the dataset as
`.geobox`, `.affine` and `.extent` respectively.

"""

import xarray
from affine import Affine

from datacube.utils import data_resolution_and_offset, geometry


def _norm_crs(crs):
    if crs is None or isinstance(crs, geometry.CRS):
        return crs
    elif isinstance(crs, str):
        return geometry.CRS(crs)
    else:
        raise ValueError('Can not interpret {} as CRS'.format(type(crs)))


def _xarray_affine(obj):
    crs = _norm_crs(obj.crs)
    if crs is None:
        return None

    dims = crs.dimensions
    xres, xoff = data_resolution_and_offset(obj[dims[1]].values)
    yres, yoff = data_resolution_and_offset(obj[dims[0]].values)
    return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)


def _xarray_extent(obj):
    geobox = obj.geobox
    return None if geobox is None else geobox.extent


def _xarray_geobox(obj):
    crs = _norm_crs(obj.crs)
    if crs is None:
        return None

    dims = crs.dimensions
    return geometry.GeoBox(obj[dims[1]].size, obj[dims[0]].size, obj.affine, crs)


xarray.Dataset.geobox = property(_xarray_geobox)
xarray.Dataset.affine = property(_xarray_affine)
xarray.Dataset.extent = property(_xarray_extent)
xarray.DataArray.geobox = property(_xarray_geobox)
xarray.DataArray.affine = property(_xarray_affine)
xarray.DataArray.extent = property(_xarray_extent)
