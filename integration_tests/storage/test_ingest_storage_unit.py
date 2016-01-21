# coding=utf-8
"""
Module
"""
from __future__ import absolute_import
from pathlib import Path

import pytest

from datacube.model import Dataset
from datacube.storage.storage import create_storage_unit_from_datasets
from datacube.scripts.ingest_storage_units import process_storage_unit


@pytest.fixture
def ingestable_storage_unit(index, example_ls5_nbar_metadata_doc, tmpdir, indexed_ls5_nbar_storage_type,
                            default_collection):
    tile_index = (150, -35)

    # strip extract measurements from storage_type
    filename = 'file://' + str(tmpdir.join('example_storage_unit.nc'))
    dataset = Dataset(default_collection,  # Collection
                      example_ls5_nbar_metadata_doc,
                      Path(str(tmpdir))
                      )

    create_storage_unit_from_datasets(tile_index, [dataset], indexed_ls5_nbar_storage_type, filename)

    create_storage_unit_from_array(storage_type, tile_index, nparray, filename)

    return filename


def test_ingest_storage_unit(ingestable_storage_unit):

    process_storage_unit(ingestable_storage_unit)
