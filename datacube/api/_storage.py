# coding=utf-8
"""
Functions for dealing with storage unit index objects and access objects.
"""
from __future__ import absolute_import, division, print_function

import datetime
from collections import defaultdict
from functools import reduce as reduce_
import itertools

import numpy

from datacube.model import Variable, time_coordinate_value
from datacube.storage.access.core import StorageUnitDimensionProxy, StorageUnitBase
from datacube.storage.access.backends import NetCDF4StorageUnit, GeoTifStorageUnit


def make_in_memory_storage_unit(su, coordinates, variables, attributes, crs):
    faux = MemoryStorageUnit(file_path=su.local_path,
                             coordinates=coordinates,
                             variables=variables,
                             attributes=attributes,
                             crs=crs)

    # TODO: Retrive irregular coords from database instead of opening file
    irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
    irregular_dims = [name for name, coord in coordinates.items()
                      if name in irregular_dim_names and coord.length > 2]

    if irregular_dims and su.storage_type.driver == 'NetCDF CF':
        real_su = NetCDF4StorageUnit(su.local_path,
                                     coordinates=coordinates, variables=variables, attributes=attributes)
        for coord in irregular_dims:
            coord_values, _ = real_su.get_coord(coord)
            faux.coordinate_values[coord] = coord_values
    return faux


def make_storage_unit_collection_from_descriptor(descriptor_su):
    return StorageUnitCollection([NetCDF4StorageUnit.from_file(su['storage_path']) for su in descriptor_su.values()])


def make_storage_unit(su, is_diskless=False, include_lineage=False):
    """convert search result into StorageUnit object
    :param su: database index storage unit
    :param is_diskless: Use a cached object for the source of data, rather than the file
    :param include_lineage: Include an 'extra_metadata' variable containing detailed lineage information.
        Note: This can cause the query to be slow for large datasets, as it is not lazy-loaded.
    """
    crs = {dim: su.descriptor['coordinates'][dim].get('units', None) for dim in su.storage_type.dimensions}
    for dim in crs.keys():
        if dim in su.storage_type.spatial_dimensions:
            crs[dim] = su.storage_type.crs
    coordinates = su.coordinates
    variables = {
        varname: Variable(
            dtype=numpy.dtype(attributes['dtype']),
            nodata=attributes.get('nodata', None),
            dimensions=su.storage_type.dimensions,
            units=attributes.get('units', None))
        for varname, attributes in su.storage_type.measurements.items()
    }
    if 'extra_metadata' not in variables.keys() and include_lineage:
        variables['extra_metadata'] = Variable(numpy.dtype('S30000'), None, ('time',), None)

    attributes = {
        'storage_type': su.storage_type,
        'dataset_ids': su.dataset_ids
    }

    if is_diskless:
        return make_in_memory_storage_unit(su,
                                           coordinates=coordinates,
                                           variables=variables,
                                           attributes=attributes,
                                           crs=crs)

    if su.storage_type.driver == 'NetCDF CF':
        return NetCDF4StorageUnit(su.local_path, coordinates=coordinates, variables=variables,
                                  attributes=attributes, crs=crs)

    if su.storage_type.driver == 'GeoTiff':
        result = GeoTifStorageUnit(su.local_path, coordinates=coordinates, variables=variables, attributes=attributes)
        time = datetime.datetime.strptime(su.descriptor['extents']['time_min'], '%Y-%m-%dT%H:%M:%S.%f')
        return StorageUnitDimensionProxy(result, time_coordinate_value(time))

    raise RuntimeError('unsupported storage unit access driver %s' % su.storage_type.driver)


def get_tiles_for_su(su):
    access_unit = make_storage_unit(su, is_diskless=True, )
    irregular_coords = []
    irregular_dims = [dim for dim in su.storage_type.dimensions if dim in ['time', 't']]
    for dim in irregular_dims:
        coords = access_unit.get_coord(dim)[0]  # TODO: Get coords from index.SU, not storage.SU
        irregular_coords.append([(dim, value) for value in coords])
    slices = itertools.product(*irregular_coords)
    return slices


class StorageUnitCollection(object):
    """Holds a list of storage units for some convenience functions"""

    def __init__(self, storage_units=None):
        self._storage_units = storage_units or []

    def append(self, storage_unit):
        self._storage_units.append(storage_unit)

    def iteritems(self):
        su_iter = iter(self._storage_units)
        for su in su_iter:
            yield su

    def items(self):
        return self._storage_units

    def get_storage_units(self):
        return self._storage_units

    def get_storage_unit_stats(self, dimensions):
        stats = {}
        irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
        for storage_unit in self._storage_units:
            index = tuple(storage_unit.coordinates[dim].begin for dim in dimensions)
            stats[index] = {
                'storage_min': tuple(min(storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end)
                                     for dim in dimensions),
                'storage_max': tuple(max(storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end)
                                     for dim in dimensions),
                'storage_shape': tuple(storage_unit.coordinates[dim].length for dim in dimensions),
                'storage_path': str(storage_unit.file_path),
                'irregular_indicies': dict((dim, storage_unit.get_coord(dim)[0].tolist())
                                           for dim in dimensions if dim in irregular_dim_names)
            }
        return stats

    def get_variables(self):
        variables = {}
        for storage_unit in self._storage_units:
            for variable_name, variable in storage_unit.variables.items():
                if len(variable.dimensions) == 3:
                    variables[variable_name] = variable
        return variables

    def group_by_dimensions(self):
        dimension_group = defaultdict(StorageUnitCollection)
        for storage_unit in self._storage_units:
            dim_groups = list(set(tuple(variable.dimensions) for variable in storage_unit.variables.values()))
            for dims in dim_groups:
                dimension_group[dims].append(storage_unit)
        return dimension_group

    def get_spatial_crs(self):
        if len(self._storage_units) == 0:
            return None
        sample = self._storage_units[0]
        crs = sample.crs
        spatial_crs = None
        for key in crs.keys():
            if key in ['latitude', 'longitude', 'lat', 'lon', 'x', 'y']:
                spatial_crs = crs[key]
        return spatial_crs


class MemoryStorageUnit(StorageUnitBase):
    def __init__(self, coordinates, variables, attributes=None, coodinate_values=None, crs=None, file_path=None):
        self.coordinates = coordinates
        self.variables = variables
        self.crs = crs or {}
        self.coordinate_values = coodinate_values or {}
        self.attributes = attributes or {}
        self.file_path = file_path

    def _get_coord(self, name):
        if name in self.coordinate_values:
            return self.coordinate_values[name]
        coord = self.coordinates[name]
        data = numpy.linspace(coord.begin, coord.end, coord.length).astype(coord.dtype)
        self.coordinate_values[name] = data
        return data

    def get_crs(self):
        crs = dict((dim, {'reference_system_unit': coord.units}) for dim, coord in self.coordinates.items())
        for coord, value in self.crs.items():
            if isinstance(coord, tuple):  # Flatten grid_mappings into per-coord units
                for c in coord:
                    crs[c]['reference_system_definition'] = value
            else:
                crs[coord]['reference_system_definition'] = value
        return crs

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        shape = tuple(self.coordinates[dim].length for dim in var.dimensions)
        size = reduce_(lambda x, y: x*y, shape, 1)
        numpy.copyto(dest, numpy.arange(size).reshape(shape)[index])
