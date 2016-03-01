# coding=utf-8
"""
Functions for breaking-up irregular dimensions into contiguous runs common across many storage units.
"""
from __future__ import absolute_import, division, print_function

import copy
import itertools

from datacube.model import Coordinate
from datacube.storage.storage import StorageUnitBase


def _stratify_storage_unit(storage_unit, dimension):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together
    :param storage_unit: A storage unit
    :param dimension: The name of the irregular dimension to stratify
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    if dimension not in storage_unit.coordinates:
        return [storage_unit]
    irregular_coord = storage_unit.coordinates[dimension]
    if irregular_coord.length > 1:
        coord, index = storage_unit.get_coord(dimension)
        return [IrregularStorageUnitSlice(storage_unit, dimension, i, coord=coord[i:i+1])
                for i, c in enumerate(coord)]
    return [storage_unit]


def _stratify_irregular_dimension(storage_units, dimension):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together
    :param storage_units:
    :param dimension:
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    stratified_units = [_stratify_storage_unit(storage_unit, dimension) for storage_unit in storage_units]
    return list(itertools.chain(*stratified_units))


class IrregularStorageUnitSlice(StorageUnitBase):
    """ Storage Unit interface for accessing another Storage unit at a defined coordinate  """
    def __init__(self, parent, dimension, index=None, irregular_slice=None, coord=None):
        self._parent = parent
        self._sliced_coordinate = dimension
        self._slice = irregular_slice or slice(index, index+1)
        self.coordinates = copy.copy(parent.coordinates)
        real_dim = self.coordinates[dimension]
        if coord is not None:
            self._cached_coord = coord
        else:
            self._cached_coord, _ = parent.get_coord(dimension, index=self._slice)

        fake_dim = Coordinate(dtype=real_dim.dtype,
                              begin=self._cached_coord[0],
                              end=self._cached_coord[0],
                              length=1,
                              units=real_dim.units)
        self.coordinates[dimension] = fake_dim
        self.variables = parent.variables
        self.file_path = parent.file_path

    def get_crs(self):
        return self._parent.get_crs()

    def get_coord(self, name, index=None):
        if name == self._sliced_coordinate:
            return self._cached_coord, slice(0, 1, 1)
        return self._parent.get_coord(name, index)

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        dim_i = var.dimensions.index(self._sliced_coordinate)
        offset = self._slice.start
        parent_index = tuple(slice(subset.start + offset, subset.stop + offset) if i == 0 else subset
                             for i, subset in enumerate(index))
        self._parent._fill_data(name, parent_index, dest)  # pylint: disable=protected-access
