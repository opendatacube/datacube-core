# coding=utf-8
"""
Functions for breaking-up irregular dimensions into contiguous runs common across many storage units.
"""
from __future__ import absolute_import, division, print_function

import copy
import itertools

import numpy

from datacube.model import Coordinate
from datacube.storage.access.core import StorageUnitBase


def stratify_storage_unit(storage_unit, dimension, runs):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together

    :param storage_unit: A storage unit
    :param dimension: The name of the irregular dimension to stratify
    :param runs: A list of ordered coordinates that make up a contiguous run across all storage units
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    if dimension not in storage_unit.coordinates:
        return [storage_unit]
    irregular_coord = storage_unit.coordinates[dimension]
    if irregular_coord.length <= 1:
        return [storage_unit]
    coord, index = storage_unit.get_coord(dimension)
    sus = list()
    start_i = 0
    current_run = []
    for i, c in enumerate(coord):
        if c not in current_run:
            if current_run:
                # If the run is not the current_run, finish it off and start a new one
                contiguous_slice = slice(start_i, i, 1)
                sus.append(IrregularStorageUnitSlice(storage_unit, dimension, i,
                                                     irregular_slice=contiguous_slice,
                                                     coord=coord[contiguous_slice]))
            start_i = i
            found_run = [run for run in runs if c in run]
            assert len(found_run) == 1
            current_run = found_run[0]

    if current_run:
        # Make sure to finish off the last one
        if start_i == 0:
            # Only create a proxy if it is needed, if it matches the run, just use it
            return [storage_unit]
        else:
            contiguous_slice = slice(start_i, len(coord), 1)
            sus.append(IrregularStorageUnitSlice(storage_unit, dimension,
                                                 irregular_slice=contiguous_slice,
                                                 coord=coord[contiguous_slice]))
    return sus
    #
    # return [IrregularStorageUnitSlice(storage_unit, dimension, i, coord=coord[i:i+1])
    #         for i, c in enumerate(coord)]


def stratify_irregular_dimension(storage_units, dimension):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together
    :param storage_units:
    :param dimension:
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    storage_units = list(storage_units)

    runs = []
    current_run = []
    sus_in_current_run = {}

    # get all coordinates along dimension, sorted
    concat_coords = numpy.concatenate(list(su.get_coord(dimension)[0] for su in storage_units))
    all_coords = numpy.unique(concat_coords)
    all_coords.sort()
    # for each coordinate:
    for coord in all_coords:
        # sus_with_coord = get all sus that have the coordinate
        sus_with_coord = {su for su in storage_units if coord in su.get_coord(dimension)[0]}
        # if sus_with_coord !=  sus_in_current_run:
        if sus_with_coord != sus_in_current_run:
            if current_run:
                runs.append(current_run)
            current_run = [coord]
            sus_in_current_run = sus_with_coord
        else:
            current_run.append(coord)

    if current_run:
        runs.append(current_run)

    stratified_units = [stratify_storage_unit(storage_unit, dimension, runs) for storage_unit in storage_units]
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
                              end=self._cached_coord[-1],
                              length=self._cached_coord.size,
                              units=real_dim.units)
        self.coordinates[dimension] = fake_dim
        self.variables = parent.variables
        self.file_path = parent.file_path

    def get_crs(self):
        return self._parent.get_crs()

    def get_coord(self, name, index=None):
        if name == self._sliced_coordinate:
            return self._cached_coord, slice(0, len(self._cached_coord), 1)
        return self._parent.get_coord(name, index)

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        dim_i = var.dimensions.index(self._sliced_coordinate)
        offset = self._slice.start
        parent_index = tuple(slice(subset.start + offset, subset.stop + offset) if dim_i == i else subset
                             for i, subset in enumerate(index))
        self._parent._fill_data(name, parent_index, dest)  # pylint: disable=protected-access
