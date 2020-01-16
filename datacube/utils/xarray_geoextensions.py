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


def _get_crs(obj):
    if not isinstance(obj, xarray.Dataset) and not isinstance(obj, xarray.DataArray):
        raise ValueError('Can not get crs from {}'.format(type(obj)))

    if isinstance(obj, xarray.Dataset):
        if len(obj.data_vars) > 0:
            data_array = next(iter(obj.data_vars.values()))
        else:
            # fall back option
            return obj.attrs.get('crs', None)
    else:
        data_array = obj

    # Assumption: (... y,x) or (...y,x,band)
    if data_array.dims[-1] == 'band':
        spatial_dims = data_array.dims[-3:-1]
    else:
        spatial_dims = data_array.dims[-2:]

    crs_set = set(data_array[d].attrs.get('crs', None) for d in spatial_dims)
    crs = None
    if len(crs_set) > 1:
        raise ValueError('Spatial dimensions have different crs.')
    elif len(crs_set) == 1:
        crs = crs_set.pop()

    if crs is None:
        # fall back option
        crs = data_array.attrs.get('crs', None) or obj.attrs.get('crs', None)
    return crs


def _xarray_affine(obj):
    crs = _norm_crs(_get_crs(obj))
    if crs is None:
        return None

    dims = crs.dimensions

    try:
        xres, xoff = data_resolution_and_offset(obj[dims[1]].values)
        yres, yoff = data_resolution_and_offset(obj[dims[0]].values)

    except ValueError:
        xres = obj[dims[1]].attrs['resolution']
        xoff = obj[dims[1]].values[0] - xres / 2
        yres = obj[dims[0]].attrs['resolution']
        yoff = obj[dims[0]].values[0] - yres / 2

    return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)


def _xarray_extent(obj):
    geobox = obj.geobox
    return None if geobox is None else geobox.extent


def _xarray_geobox(obj):
    crs = _norm_crs(_get_crs(obj))
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
