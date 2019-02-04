# coding=utf-8
"""
Reproject :class:`xarray.DataArray` objects.

Makes assumptions on the data that it matches certain NetCDF-CF criteria
The CRS is stored as the 'spatial_ref' attribute of the 'crs' data variable
Spatial dimensions are either 'latitude' / 'longitude' or 'x' / 'y',
although this should probably instead check the 'standard_name' as defined by CF
"""

import copy

import numpy as np

import rasterio
import rasterio.warp

try:
    from rasterio.warp import Resampling
except ImportError:
    from rasterio.warp import RESAMPLING as Resampling
from rasterio import Affine

import xarray as xr


def reproject_like(src_data_array, like_data_array, resampling=Resampling.nearest):
    """
    Reproject a DataArray object to match the resolution and projection of another DataArray.

    Note: Only 2D arrays with dimensions named 'latitude'/'longitude' or 'x'/'y' are currently supported.
    Requires an attr 'spatial_ref' to be set containing a valid CRS.
    If using a WKT (e.g. from spatiareference.org), make sure it is an OGC WKT.

    :param src_data_array: a `xarray.DataArray` that will be reprojected
    :param like_data_array: a `xarray.DataArray` of the target resolution and projection
    :return: a `xarray.DataArray` containing the data from the src_data_array, reprojected to match like_data_array
    """
    src_crs = src_data_array.attrs['spatial_ref']
    dest_crs = like_data_array.attrs['spatial_ref']

    if 'latitude' in like_data_array.dims and 'longitude' in like_data_array.dims:
        dest_x_dim = 'longitude'
        dest_y_dim = 'latitude'
    elif 'x' in like_data_array.dims and 'y' in like_data_array.dims:
        dest_x_dim = 'x'
        dest_y_dim = 'y'
    else:
        raise ValueError

    src_width = like_data_array[dest_x_dim].size - 1
    src_height = like_data_array[dest_y_dim].size - 1

    src_left = float(like_data_array[dest_x_dim][0])
    src_right = float(like_data_array[dest_x_dim][-1])
    src_top = float(like_data_array[dest_y_dim][0])
    src_bottom = float(like_data_array[dest_y_dim][-1])

    dest_resolution_x = (src_right - src_left) / src_width
    dest_resolution_y = (src_bottom - src_top) / src_height
    dest_resolution = (dest_resolution_x + dest_resolution_y) / 2

    return reproject(src_data_array, src_crs, dest_crs, dest_resolution, resampling=resampling)


def reproject(src_data_array, src_crs, dst_crs, resolution=None, resampling=Resampling.nearest,
              set_nan=False, copy_attrs=True):
    """
    Reproject :class:`xarray.DataArray` objects

    Note: Only 2D arrays with dimensions named 'latitude'/'longitude' or 'x'/'y' are currently supported.
    Requires an attr 'spatial_ref' to be set containing a valid CRS.
    If using a WKT (e.g. from spatiareference.org), make sure it is an OGC WKT.

    :param src_data_array: `xarray.DataArray`
    :param src_crs: EPSG code, OGC WKT string, etc
    :param dst_crs: EPSG code, OGC WKT string, etc
    :param resolution: Size of a destination pixel in destination projection units (eg degrees or metres)
    :param resampling: Resampling method - see rasterio.warp.reproject for more details
        Possible values are:
            Resampling.nearest,
            Resampling.bilinear,
            Resampling.cubic,
            Resampling.cubic_spline,
            Resampling.lanczos,
            Resampling.average,
            Resampling.mode
    :param set_nan: If nodata values from the source and any nodata areas created by the reproject should be set to NaN
        Note: this causes the data type to be cast to float.
    :param copy_attrs: Should the attributes be copied to the destination.
        Note: No attempt is made to update spatial attributes, e.g. spatial_ref, bounds, etc
    :return: A reprojected :class:`xarray.DataArray`
    """
    # TODO: Support lazy loading of data with dask imperative function
    src_data = np.copy(src_data_array.load().data)

    src_affine = _make_src_affine(src_data_array)
    dst_affine, dst_width, dst_height = _make_dst_affine(src_data_array, src_crs, dst_crs, resolution)

    dst_data = np.zeros((dst_height, dst_width), dtype=src_data_array.dtype)
    nodata = _get_nodata_value(src_data_array) or -999
    rasterio.warp.reproject(source=src_data,
                            destination=dst_data,
                            src_transform=src_affine,
                            src_crs=src_crs,
                            src_nodata=nodata,
                            dst_transform=dst_affine,
                            dst_crs=dst_crs,
                            dst_nodata=nodata,
                            resampling=resampling)
    if set_nan:
        dst_data = dst_data.astype(np.float)
        dst_data[dst_data == nodata] = np.nan

    return xr.DataArray(data=dst_data,
                        coords=_make_coords(src_data_array, dst_affine, dst_width, dst_height),
                        dims=copy.deepcopy(src_data_array.dims),
                        attrs=copy.deepcopy(src_data_array.attrs) if copy_attrs else None)


def append_solar_day(dataset, longitude=None):
    """
    Append a ``solar_day`` data variable on the given dataset.

    The resulting dataset could then have ``groupby`` operations performed on it, such as finding the max value for
    each day::
        dataset = api.get_dataset(...)
        geo_xarray.append_solar_day(dataset)
        solar_day_data = dataset.groupby('solar_day').max(dim='time')

    :param dataset: An ``xarray.Dataset`` with a ``time`` dimension.

    If a ``longitude`` parameter is not specified, the dataset must also contain a spatial dimensions (i.e. ``x, y`` or
    ``longitude, latitude``) and a ``crs`` variable with a ``spatial_ref`` attribute.
    :param longitude: mean longitude of the dataset in WGS84
    """
    if longitude is None:
        longitude = _get_mean_longitude(dataset)
    solar_days = np.array([_solar_day(dt, longitude) for dt in dataset.time.values]).astype('datetime64[D]')
    dataset['solar_day'] = xr.DataArray(solar_days, coords={'time': dataset.time}, dims=['time'])


def _solar_day(utc, latitude):
    seconds_per_degree = 240
    offset_seconds = int(latitude * seconds_per_degree)
    offset = np.timedelta64(offset_seconds, 's')
    return utc + offset


def _get_mean_longitude(dataset):
    x, y = _get_spatial_dims(dataset)
    mean_lat = float(dataset[x][0] + dataset[x][-1]) / 2.
    mean_lon = float(dataset[y][0] + dataset[y][-1]) / 2.
    bounds = {'left': mean_lon, 'right': mean_lon, 'top': mean_lat, 'bottom': mean_lat}
    left, bottom, right, top = rasterio.warp.transform_bounds(str(dataset.crs), 'EPSG:4326', **bounds)
    return left


def _make_coords(src_data_array, dst_affine, dst_width, dst_height):
    coords = copy.deepcopy(src_data_array.coords)
    new_coords = _warp_spatial_coords(src_data_array, dst_affine, dst_width, dst_height)
    coords.update(new_coords)
    return coords


def _make_dst_affine(src_data_array, src_crs, dst_crs, dst_resolution=None):
    src_bounds = _get_bounds(src_data_array)
    src_width, src_height = _get_shape(src_data_array)
    dst_affine, dst_width, dst_height = rasterio.warp.calculate_default_transform(src_crs, dst_crs,
                                                                                  src_width, src_height,
                                                                                  *src_bounds,
                                                                                  resolution=dst_resolution)
    return dst_affine, dst_width, dst_height


def _make_src_affine(src_data_array):
    src_bounds = _get_bounds(src_data_array)
    src_left, src_bottom, src_right, src_top = src_bounds
    src_resolution_x, src_resolution_y = _get_resolution(src_data_array, as_tuple=True)
    return Affine.translation(src_left, src_top) * Affine.scale(src_resolution_x, src_resolution_y)


def _get_spatial_dims(data_array):
    if 'latitude' in data_array.dims and 'longitude' in data_array.dims:
        x_dim = 'longitude'
        y_dim = 'latitude'
    elif 'x' in data_array.dims and 'y' in data_array.dims:
        x_dim = 'x'
        y_dim = 'y'
    else:
        raise KeyError

    return x_dim, y_dim


def _get_bounds(data_array):
    x_dim, y_dim = _get_spatial_dims(data_array)

    left = float(data_array[x_dim][0])
    right = float(data_array[x_dim][-1])
    top = float(data_array[y_dim][0])
    bottom = float(data_array[y_dim][-1])

    return left, bottom, right, top


def _get_shape(data_array):
    x_dim, y_dim = _get_spatial_dims(data_array)
    return data_array[x_dim].size, data_array[y_dim].size


def _get_resolution(data_array, get_avg_res=True, as_tuple=False):
    left, bottom, right, top = _get_bounds(data_array)
    width, height = _get_shape(data_array)

    resolution_x = (right - left) / (width - 1)
    resolution_y = (bottom - top) / (height - 1)
    if as_tuple:
        resolution = (resolution_x, resolution_y)
    elif get_avg_res:
        resolution = (resolution_x + resolution_y) / 2
    else:
        assert resolution_x == resolution_y
        resolution = resolution_x
    return resolution


def _get_nodata_value(data_array):
    nodata = (data_array.attrs.get('_FillValue') or
              data_array.attrs.get('missing_value') or
              data_array.attrs.get('fill_value'))
    return nodata


def _warp_spatial_coords(data_array, affine, width, height):
    ul = affine * (0, 0)
    lr = affine * (width, height)
    x_coords = np.linspace(ul[0], lr[0], num=width)
    y_coords = np.linspace(ul[1], lr[1], num=height)
    x_dim, y_dim = _get_spatial_dims(data_array)

    coords = {
        x_dim: x_coords,
        y_dim: y_coords
    }

    return coords
