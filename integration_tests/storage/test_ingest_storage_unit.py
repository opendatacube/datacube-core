# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datetime import datetime
from pathlib import Path

import pytest

from datacube.model import GeoBox
from datacube.scripts.ingest_storage_units import process_storage_unit


class SingleTimeEmptyDataProvider(object):
    def __init__(self, storage_type, tile_index):
        self.tile_spec = GeoBox.from_storage_type(storage_type, tile_index)
        self.storage_type = storage_type

    def get_metadata_documents(self):
        return []

    def get_time_values(self):
        return [datetime(2000, 1, 1)]

    def write_data_to_storage_unit(self, su_writer):
        for _, measurement_descriptor in self.storage_type.measurements.items():
            out_var = su_writer.ensure_variable(measurement_descriptor, self.storage_type.chunking)
            out_var[0] = 0


# @pytest.fixture
# def ingestable_storage_unit(index, tmpdir, indexed_ls5_nbar_storage_type,
#                             default_collection):
#     tile_index = (150, -35)
#
#     filename = str(tmpdir.join('example_storage_unit.nc'))
#
#     data_provider = SingleTimeEmptyDataProvider(indexed_ls5_nbar_storage_type, tile_index)
#
#     write_storage_unit_to_disk(filename, data_provider)
#
#     return filename


@pytest.skip(reason="Not yet implemented")
def test_ingest_storage_unit(ingestable_storage_unit, index):
    assert Path(ingestable_storage_unit).exists()
    process_storage_unit(ingestable_storage_unit, index)
