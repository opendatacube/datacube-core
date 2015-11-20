# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import pytest

from datacube import model
from . import util


def test_expected_metadata_path():
    files = util.write_files({
        'directory_dataset': {'file1.txt': 'test'},
        'dataset_metadata.yaml': 'test'
    })

    # A dataset directory will have an internal 'ga-metadata.yaml' file.
    doc_type, path = model._expected_metadata_path(files.joinpath('directory_dataset'))
    assert doc_type == 'eo'
    assert path.absolute() == files.joinpath('directory_dataset', 'ga-metadata.yaml').absolute()

    # When a file is specified it is a yaml dataset metadata directory and it's path is simply returned
    doc_type, path = model._expected_metadata_path(files.joinpath('dataset_metadata.yaml'))
    assert doc_type == 'eo'
    assert path.absolute() == files.joinpath('dataset_metadata.yaml').absolute()

    # Nonexistent dataset raises a ValueError.
    with pytest.raises(ValueError):
        model._expected_metadata_path(files.joinpath('missing-dataset.tif'))
