from collections import namedtuple

import netCDF4

Coordinate = namedtuple('Coordinate', ('dtype', 'begin', 'end', 'length'))
Variable = namedtuple('Variable', ('dtype', 'ndv', 'coordinates'))


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
    collection_description = []
    for filename in filenames:
        coordinates, variables = read_netcdf_structure(filename)

        collection_description.append({
            'filename': filename,
            'coordinates': coordinates,
            'variables': variables
        })
    return collection_description


def read_netcdf_structure(filename):
    """
    Read a netcdf4 file and return a dicts describing its coordinates and variables

    :param filename:
    :return:
    """
    with netCDF4.Dataset(filename) as nco:
        coordinates = {}
        variables = {}

        for name, var in nco.variables.items():
            dims = var.dimensions
            name = str(name)
            if len(dims) == 1 and name == dims[0]:
                coordinates[name] = {
                    'dtype': str(var.dtype),
                    'begin': var[0].item(),
                    'end': var[-1].item(),
                    'length': var.shape[0]
                }
            else:
                ndv = getattr(var, 'missing_value', None) or getattr(var, 'fill_value', None)
                if ndv:
                    ndv = ndv.item()
                variables[name] = {
                    'dtype': str(var.dtype),
                    'ndv': ndv,
                    'dimensions': [str(dim) for dim in var.dimensions]
                }
    return coordinates, variables
