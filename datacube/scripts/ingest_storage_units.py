#!/usr/bin/env python
# coding=utf-8
"""
Ingest storage units from the command-line.
"""
from __future__ import absolute_import

from pathlib import Path

import click
import netCDF4
import yaml

from datacube.ingest import find_storage_types_for_datasets
from datacube.model import StorageUnit
from datacube.storage.access.backends import NetCDF4StorageUnit
from datacube.ui import click as ui
from datacube.ui.click import CLICK_SETTINGS
from datacube.utils import namedtuples2dicts


@click.command(help="Ingest storage units into the Data Cube.", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
@click.argument('storage_units',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index(app_name='su-ingest')
def cli(index, storage_units):
    for storage_unit_path in storage_units:
        process_storage_unit(storage_unit_path, index=index)


if __name__ == '__main__':
    cli()


def read_netcdf_structure(filename):
    """
    Read a netcdf4 file and return a dict describing its coordinates and variables

    :param filename:
    :return:
    """
    ncsu = NetCDF4StorageUnit.from_file(filename)

    extents = {k: ncsu.attributes[k] for k in
               [u'geospatial_lat_max', u'geospatial_lat_min', u'geospatial_lon_max', u'geospatial_lon_min']}

    time_units = ncsu.coordinates[u'time'].units

    extents['time_min'] = netCDF4.num2date(ncsu.coordinates['time'].begin, time_units)
    extents['time_max'] = netCDF4.num2date(ncsu.coordinates['time'].end, time_units)

    coordinates = namedtuples2dicts(ncsu.coordinates)

    return dict(coordinates=coordinates, extents=extents)


def process_storage_unit(filename, index):
    nco = open_storage_unit(filename)
    datasets = list(pull_datasets_from_storage_unit(nco))

    if len(datasets) == 0:
        raise RuntimeError("No datasets found in storage unit {}. Unable to ingest.".format(filename))
    elif len(datasets) > 1:
        raise RuntimeError("Multiple datasets found in storage unit {}. Unable to ingest.".format(filename))

    add_datasets_to_index(datasets, index)

    storage_types = find_storage_types_for_datasets(datasets, index)

    storage_unit = create_in_memory_storage_unit(datasets, storage_types, filename)

    add_storage_unit_to_index(storage_unit, index)


def open_storage_unit(path):
    return netCDF4.Dataset(path)


def pull_datasets_from_storage_unit(storage_unit):
    if 'extra_metadata' not in storage_unit.variables:
        raise StopIteration
    raw_datasets = storage_unit.variables['extra_metadata']
    for parsed_doc in yaml.safe_load_all(raw_datasets):
        yield parsed_doc


def add_datasets_to_index(datasets, index):
    for dataset in datasets:
        index.datasets.add(dataset)


def create_in_memory_storage_unit(datasets, storage_type, filename):
    su_descriptor = read_netcdf_structure(filename)
    size_bytes = Path(filename).stat().st_size
    return StorageUnit([dataset.id for dataset in datasets],
                       storage_type,
                       su_descriptor,
                       size_bytes=size_bytes,
                       relative_path=storage_type.local_path_to_location_offset('file://' + filename))


def add_storage_unit_to_index(storage_unit, index):
    index.storage.add(storage_unit)
