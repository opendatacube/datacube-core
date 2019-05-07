# coding=utf-8
"""
Module
"""

import pytest

from datacube.ui.common import get_metadata_path, find_any_metadata_suffix
from datacube.testutils import write_files, assert_file_structure


def test_get_metadata_path():
    FILES = {
        'directory_dataset': {
            'file1.txt': '',
            'file2.txt': '',
            'agdc-metadata.yaml.gz': ''
        },
        'file_dataset.tif': '',
        'file_dataset.tif.agdc-md.yaml': '',
        'dataset_metadata.yaml': '',
        'no_metadata.tif': '',
    }

    out_dir = write_files(FILES)

    assert_file_structure(out_dir, FILES)

    # A metadata file can be specified directly.
    path = get_metadata_path(out_dir.joinpath('dataset_metadata.yaml'))
    assert path.absolute() == out_dir.joinpath('dataset_metadata.yaml').absolute()

    # A dataset directory will have an internal 'agdc-metadata' file.
    path = get_metadata_path(out_dir.joinpath('directory_dataset'))
    assert path.absolute() == out_dir.joinpath('directory_dataset', 'agdc-metadata.yaml.gz').absolute()

    # Other out_dir can have a sibling file ending in 'agdc-md.yaml'
    path = get_metadata_path(out_dir.joinpath('file_dataset.tif'))
    assert path.absolute() == out_dir.joinpath('file_dataset.tif.agdc-md.yaml').absolute()

    # Lack of metadata raises an error.
    with pytest.raises(ValueError):
        get_metadata_path(out_dir.joinpath('no_metadata.tif'))

    # Nonexistent dataset raises a ValueError.
    with pytest.raises(ValueError):
        get_metadata_path(out_dir.joinpath('missing-dataset.tif'))


def test_find_any_metatadata_suffix():
    files = write_files({
        'directory_dataset': {
            'file1.txt': '',
            'file2.txt': '',
            'agdc-metadata.json.gz': ''
        },
        'file_dataset.tif.agdc-md.yaml': '',
        'dataset_metadata.YAML': '',
        'no_metadata.tif': '',
        'ambigous.yml': '',
        'ambigous.yaml': '',
    })

    path = find_any_metadata_suffix(files.joinpath('dataset_metadata'))
    assert path.absolute() == files.joinpath('dataset_metadata.YAML').absolute()

    path = find_any_metadata_suffix(files.joinpath('directory_dataset', 'agdc-metadata'))
    assert path.absolute() == files.joinpath('directory_dataset', 'agdc-metadata.json.gz').absolute()

    path = find_any_metadata_suffix(files.joinpath('file_dataset.tif.agdc-md'))
    assert path.absolute() == files.joinpath('file_dataset.tif.agdc-md.yaml').absolute()

    # Returns none if none exist
    path = find_any_metadata_suffix(files.joinpath('no_metadata'))
    assert path is None

    with pytest.raises(ValueError):
        find_any_metadata_suffix(files.joinpath('ambigous'))
