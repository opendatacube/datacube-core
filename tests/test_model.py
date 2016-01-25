# coding=utf-8
import os

import pytest

from datacube.model import _uri_to_local_path, Dataset


def test_uri_to_local_path():
    if os.name == 'nt':
        assert 'C:\\tmp\\test.tmp' == str(_uri_to_local_path('file:///C:/tmp/test.tmp'))

    else:
        assert '/tmp/something.txt' == str(_uri_to_local_path('file:///tmp/something.txt'))

    assert _uri_to_local_path(None) is None

    with pytest.raises(ValueError):
        _uri_to_local_path('ftp://example.com/tmp/something.txt')


def test_doctest_local_path():
    if os.name == 'nt':
        dataset = Dataset(None, None, 'file:///C:/tmp/test.tmp')

        assert str(dataset.local_path) == 'C:\\tmp\\test.tmp'

    else:
        dataset = Dataset(None, None, 'file:///tmp/something.txt')

        assert str(dataset.local_path) == '/tmp/something.txt'

    dataset = Dataset(None, None, None).local_path is None

    with pytest.raises(ValueError):
        Dataset(None, None, 'ftp://example.com/tmp/something.txt').local_path
