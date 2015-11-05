# coding=utf-8
"""
Useful methods for tests (particularly: reading/writing and checking files)
"""
from __future__ import absolute_import

import atexit
import os
import shutil
import tempfile

import pathlib
from datacube import compat


def assert_file_structure(folder, expected_structure, root=''):
    """
    Assert that the contents of a folder (filenames and subfolder names recursively)
    match the given nested dictionary structure.

    :type folder: pathlib.Path
    :type expected_structure: dict[str,str|dict]
    """

    expected_filenames = set(expected_structure.keys())
    actual_filenames = {f.name for f in folder.iterdir()}

    if expected_filenames != actual_filenames:
        missing_files = expected_filenames - actual_filenames
        missing_text = 'Missing: %r' % (sorted(list(missing_files)))
        extra_files = actual_filenames - expected_filenames
        added_text = 'Extra  : %r' % (sorted(list(extra_files)))
        raise AssertionError('Folder mismatch of %r\n\t%s\n\t%s' % (root, missing_text, added_text))

    for k, v in expected_structure.items():
        id_ = '%s/%s' % (root, k) if root else k

        f = folder.joinpath(k)
        if isinstance(v, dict):
            assert f.is_dir(), "%s is not a dir" % (id_,)
            assert_file_structure(f, v, id_)
        elif isinstance(v, compat.string_types):
            assert f.is_file(), "%s is not a file" % (id_,)
        else:
            assert False, "Only strings and dicts expected when defining a folder structure."


def write_files(file_dict):
    """
    Convenience method for writing a bunch of files to a temporary directory.

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    writeFiles({'test.txt': 'contents of text file'})

    :type file_dict: dict
    :rtype: pathlib.Path
    :return: Created temporary directory path
    """
    containing_dir = tempfile.mkdtemp(suffix='neotestrun')
    _write_files_to_dir(containing_dir, file_dict)

    def remove_if_exists(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, containing_dir)
    return pathlib.Path(containing_dir)


def _write_files_to_dir(directory_path, file_dict):
    """
    Convenience method for writing a bunch of files to a given directory.

    :type directory_path: str
    :type file_dict: dict
    """
    for filename, contents in file_dict.items():
        path = os.path.join(directory_path, filename)
        if isinstance(contents, dict):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, 'w') as f:
                if isinstance(contents, list):
                    f.writelines(contents)
                elif isinstance(contents, compat.string_types):
                    f.write(contents)
                else:
                    raise Exception('Unexpected file contents: %s' % type(contents))


def temp_dir():
    """
    Create and return a temporary directory that will be deleted automatically on exit.

    :rtype: pathlib.Path
    """
    return write_files({})


def temp_file(suffix=""):
    """
    Get a temporary file path that will be cleaned up on exit.

    Simpler than NamedTemporaryFile--- just a file path, no open mode or anything.
    :return:
    """
    f = tempfile.mktemp(suffix=suffix)

    def permissive_ignore(file_):
        if os.path.exists(file_):
            os.remove(file_)

    atexit.register(permissive_ignore, f)
    return f


def file_of_size(path, size_mb):
    """
    Create a blank file of the given size.
    """
    with open(path, "wb") as f:
        f.seek(size_mb * 1024 * 1024 - 1)
        f.write("\0")
