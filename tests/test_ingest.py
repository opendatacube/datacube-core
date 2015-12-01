# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import pytest
from datacube import ingest
from . import util


def test_expected_metadata_path():
    files = util.write_files({
        'directory_dataset': {'file1.txt': 'test'},
        'file_dataset.tif': 'test',
        'dataset_metadata.yaml': 'test'
    })

    # A dataset directory will have an internal 'ga-metadata.yaml' file.
    path = ingest._expected_metadata_path(files.joinpath('directory_dataset'))
    assert path.absolute() == files.joinpath('directory_dataset', 'ga-metadata.yaml').absolute()

    # When a file is specified it is a yaml dataset metadata directory and it's path is simply returned
    path = ingest._expected_metadata_path(files.joinpath('dataset_metadata.yaml'))
    assert path.absolute() == files.joinpath('dataset_metadata.yaml').absolute()

    # Other files will have a sibling file ending in 'ga-md.yaml'
    path = ingest._expected_metadata_path(files.joinpath('file_dataset.tif'))
    assert path.absolute() == files.joinpath('file_dataset.tif.ga-md.yaml').absolute()

    # Nonexistent dataset raises a ValueError.
    with pytest.raises(ValueError):
        ingest._expected_metadata_path(files.joinpath('missing-dataset.tif'))
