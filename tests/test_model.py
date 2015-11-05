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
        'file_dataset.tif': 'test'
    })

    # A dataset directory will have an internal 'ga-metadata.yaml' file.
    doc_type, path = model._expected_metadata_path(files.joinpath('directory_dataset'))
    assert doc_type == 'eo'
    assert path.absolute() == files.joinpath('directory_dataset', 'ga-metadata.yaml').absolute()

    # A dataset file will have a sibling file ending in 'ga-md.yaml'
    doc_type, path = model._expected_metadata_path(files.joinpath('file_dataset.tif'))
    assert doc_type == 'eo'
    assert path.absolute() == files.joinpath('file_dataset.tif.ga-md.yaml').absolute()

    # Nonexistent dataset raises a ValueError.
    with pytest.raises(ValueError):
        model._expected_metadata_path(files.joinpath('missing-dataset.tif'))
