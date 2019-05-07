"""
Module
"""
from pathlib import Path

import pytest

from datacube.testutils import write_files, assert_file_structure
from datacube.ui.common import get_metadata_path, _find_any_metadata_suffix, ui_path_doc_stream


def test_get_metadata_path():
    test_file_structure = {
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

    out_dir = write_files(test_file_structure)

    assert_file_structure(out_dir, test_file_structure)

    # A metadata file can be specified directly.
    path = get_metadata_path(out_dir.joinpath('dataset_metadata.yaml'))
    assert Path(path).absolute() == out_dir.joinpath('dataset_metadata.yaml').absolute()

    # A dataset directory will have an internal 'agdc-metadata' file.
    path = get_metadata_path(out_dir.joinpath('directory_dataset'))
    assert Path(path).absolute() == out_dir.joinpath('directory_dataset', 'agdc-metadata.yaml.gz').absolute()

    # Other out_dir can have a sibling file ending in 'agdc-md.yaml'
    path = get_metadata_path(out_dir.joinpath('file_dataset.tif'))
    assert Path(path).absolute() == out_dir.joinpath('file_dataset.tif.agdc-md.yaml').absolute()

    # URLs are always themselves
    example_url = 'http://localhost/dataset.yaml'
    url = get_metadata_path(example_url)
    assert url == example_url

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

    path = _find_any_metadata_suffix(files.joinpath('dataset_metadata'))
    assert Path(path).absolute() == files.joinpath('dataset_metadata.YAML').absolute()

    path = _find_any_metadata_suffix(files.joinpath('directory_dataset', 'agdc-metadata'))
    assert Path(path).absolute() == files.joinpath('directory_dataset', 'agdc-metadata.json.gz').absolute()

    path = _find_any_metadata_suffix(files.joinpath('file_dataset.tif.agdc-md'))
    assert Path(path).absolute() == files.joinpath('file_dataset.tif.agdc-md.yaml').absolute()

    # Returns none if none exist
    path = _find_any_metadata_suffix(files.joinpath('no_metadata'))
    assert path is None

    with pytest.raises(ValueError):
        _find_any_metadata_suffix(files.joinpath('ambigous'))


def test_ui_path_doc_stream(httpserver):
    filename = 'dataset_metadata.yaml'
    file_content = ''
    out_dir = write_files({filename: file_content})

    httpserver.expect_request(filename).respond_with_data(file_content)

    input_paths = [Path(out_dir) / 'dataset_metadata.yaml', httpserver.url_for(filename)]

    for input_path, (doc, resolved_path) in zip(input_paths, ui_path_doc_stream(input_paths)):
        assert doc == {}
        assert input_path == resolved_path
