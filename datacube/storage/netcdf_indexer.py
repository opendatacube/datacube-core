from __future__ import absolute_import

import netCDF4
from netCDF4 import num2date


def index_netcdfs(filenames):
    """
    Create an index of a list of netcdf files

    Returns a list of the following structure describing each file:
    {  # Must store entire file, can't handle a slice of a file
        'filename': '/path/to/filename1.nc',
        'coordinates': {
            'latitude': {
                'dtype': 'float32',
                'begin': '0',  # inclusive
                'end': '4000',  # inclusive
                'length': '4000'
            },
            ...
        },
        'variables': {
            'band1': {
                'dtype': 'float32',
                'ndv': '-999',
                'dimensions': ['time', 'latitude', 'longitude']  # order matters
            },
            ...
        }
    }

    :param filenames:
    :return: list of file description dicts
    """
    files_descriptions = {}

    for filename in filenames:
        files_descriptions[filename] = read_netcdf_structure(filename)

    return files_descriptions


def skip_variable(var):
    return not hasattr(var, 'units')


def read_netcdf_structure(filename):
    """
    Read a netcdf4 file and return a dicts describing its coordinates and variables

    :param filename:
    :return:
    """
    with netCDF4.Dataset(filename) as nco:
        coordinates = {}
        measurements = {}
        extents = {k: getattr(nco, k) for k in
                   ['geospatial_lat_max', 'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lon_min']}

        time_units = nco.variables['time'].units

        extents['time_min'] = num2date(nco.variables['time'][0], time_units)
        extents['time_max'] = num2date(nco.variables['time'][-1], time_units)

        skipped_variables = ('crs', 'extra_metadata')

        for name, var in nco.variables.items():
            if skip_variable(var):
                continue

            dims = var.dimensions
            name = str(name)
            if len(dims) == 1 and name == dims[0]:
                coordinates[name] = {
                    'dtype': str(var.dtype),
                    'begin': var[0].item(),
                    'end': var[var.size - 1].item(),
                    'length': var.shape[0]  # can't use size directly, it's a numpy.scalar
                }
            else:
                ndv = getattr(var, 'missing_value', None) or getattr(var, '_FillValue', None)
                if ndv:
                    ndv = ndv.item()
                measurements[name] = {
                    'dtype': str(var.dtype),
                    'units': str(var.units),
                    'ndv': ndv,
                    'dimensions': [str(dim) for dim in var.dimensions]
                }

    return dict(coordinates=coordinates, measurements=measurements, extents=extents)
