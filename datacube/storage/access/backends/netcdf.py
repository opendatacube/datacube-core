#    Copyright 2015 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


from __future__ import absolute_import, division, print_function

import contextlib
import threading

import numpy
import netCDF4 as nc4

from ..core import StorageUnitBase
from datacube.model import Coordinate, Variable
from ..indexing import Range, range_to_index, normalize_index

_GLOBAL_LOCK = threading.RLock()


def _open_dataset(filepath):
    """
    :type filepath: pathlib.Path
    """
    ncds = nc4.Dataset(str(filepath), mode='r', clobber=False, diskless=False, persist=False, format='NETCDF4')
    ncds.set_auto_mask(False)
    return ncds


def _get_dims_and_dtype(var):
    if var.dtype == str or var.dtype.kind == 'S':
        fake_dim = [d for d in var.dimensions if d.endswith('nchar')]
        if fake_dim:
            dims = tuple(d for d in var.dimensions if d not in fake_dim)
            fake_dim_index = var.dimensions.index(fake_dim[0])
            fake_size = var.shape[fake_dim_index]
            dtype = numpy.dtype('<S{}'.format(fake_size))
            return dims, dtype
    return tuple(var.dimensions), var.dtype


def _make_crs(grid_mappings, standard_names):
    crs = {}
    if grid_mappings:
        for standard_name, real_name in standard_names.items():
            if standard_name in ['latitude', 'longitude'] and 'latitude_longitude' in grid_mappings:
                crs[real_name] = grid_mappings['latitude_longitude']
            elif standard_name in ['projection_x_coordinate', 'projection_y_coordinate']:
                crs[real_name] = grid_mappings[list(grid_mappings.keys())[0]]
    return crs


class NetCDF4StorageUnit(StorageUnitBase):
    def __init__(self, file_path, variables, coordinates, attributes=None, crs=None):
        """
        :param variables: variables in the SU. dict of name: Variable
        :param coordinates: coordinates in the SU
        :type file_path: pathlib.Path
        """
        self.file_path = file_path
        self.coordinates = coordinates
        self.variables = variables
        self.attributes = attributes or {}
        self.crs = crs or {}
        self._coord_values = {coord_name: None for coord_name in coordinates}

    def get_crs(self):
        # Use units for sensible default
        crs = dict((dim, {'reference_system_unit': coord.units}) for dim, coord in self.coordinates.items())
        for coord, value in self.crs.items():
            crs[coord]['reference_system_definition'] = value
        return crs

    @classmethod
    def from_file(cls, file_path):
        coordinates = {}
        variables = {}
        grid_mappings = {}
        standard_names = {}

        with _GLOBAL_LOCK, contextlib.closing(_open_dataset(file_path)) as ncds:
            attributes = {k: getattr(ncds, k) for k in ncds.ncattrs()}
            for name, var in ncds.variables.items():
                dims = var.dimensions
                units = getattr(var, 'units', None)
                if hasattr(var, 'grid_mapping_name') and hasattr(var, 'spatial_ref'):
                    grid_mappings[getattr(var, 'grid_mapping_name', None)] = getattr(var, 'spatial_ref', None)
                elif len(dims) == 1 and name == dims[0]:
                    coordinates[name] = Coordinate(dtype=numpy.dtype(var.dtype),
                                                   begin=var[0].item(), end=var[var.size-1].item(),
                                                   length=var.shape[0], units=units)
                    standard_name = getattr(var, 'standard_name', None)
                    if standard_name:
                        standard_names[standard_name] = name
                else:
                    dims, dtype = _get_dims_and_dtype(var)
                    ndv = (getattr(var, '_FillValue', None) or
                           getattr(var, 'missing_value', None) or
                           getattr(var, 'fill_value', None))
                    ndv = ndv.item() if ndv else None
                    variables[name] = Variable(dtype, ndv, dims, units)
        crs = _make_crs(grid_mappings, standard_names)
        return cls(file_path, variables=variables, coordinates=coordinates, attributes=attributes, crs=crs)

    def get_coord(self, dim, index=None):
        coord = self.coordinates[dim]
        index = normalize_index(coord, index)

        if isinstance(index, slice):
            if self._coord_values[dim] is None:
                with _GLOBAL_LOCK, contextlib.closing(_open_dataset(self.file_path)) as ncds:
                    self._coord_values[dim] = ncds[dim][:]
            return self._coord_values[dim][index], index

        if isinstance(index, Range):
            if self._coord_values[dim] is None:
                with _GLOBAL_LOCK, contextlib.closing(_open_dataset(self.file_path)) as ncds:
                    self._coord_values[dim] = ncds[dim][:]
            index = range_to_index(self._coord_values[dim], index)
            return self._coord_values[dim][index], index

    def _fill_data(self, name, index, dest):
        with _GLOBAL_LOCK, contextlib.closing(_open_dataset(self.file_path)) as ncds:
            if dest.dtype.kind == 'S' and dest.dtype.itemsize > 1:
                dest = dest.view('S1').reshape(dest.shape + (-1,))
            numpy.copyto(dest, ncds[name][index])

    def get_chunk(self, name, index):
        with _GLOBAL_LOCK, contextlib.closing(_open_dataset(self.file_path)) as ncds:
            return ncds[name][index]
