"""
Useful functions for Datacube users

Not used internally, those should go in `utils.py`
"""
import rasterio

DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'count': 4,
    'driver': 'GTiff',
    'dtype': 'uint8',
    'height': 42720,
    'interleave': 'band',
    'nodata': 0.0,
    'tiled': True}


def write_geotiff(filename, dataset, time_index=None):
    """
    Write an xarray dataset to a geotiff

    :attr bands: ordered list of dataset names
    """

    dtypes = {val.dtype for val in dataset.data_vars.values()}
    assert len(dtypes) == 1  # Check for multiple dtypes

    profile = DEFAULT_PROFILE.copy()
    profile.update({
        'width': dataset.dims[dataset.crs.dimensions[1]],
        'height': dataset.dims[dataset.crs.dimensions[0]],
        'affine': dataset.affine,
        'crs': dataset.crs.crs_str,
        'count': len(dataset.data_vars),
        'dtype': str(dtypes.pop()),
        'photometric': 'RGB'
    })

    with rasterio.open(filename, 'w', **profile) as dest:
        for bandnum, data in enumerate(dataset.data_vars.values(), start=1):
            dest.write(data.isel(time=time_index).data, bandnum)
