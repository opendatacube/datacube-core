from __future__ import print_function, absolute_import

import pytest


def test_can_import_drivermanager():
    from datacube.drivers.manager import DriverManager
    assert DriverManager is not None


def test_can_import_s3_driver():
    pytest.importorskip('SharedArray')
    from datacube.drivers.s3.driver import S3Driver
    assert S3Driver is not None


def test_can_import_s3testdriver():
    pytest.importorskip('SharedArray')
    from datacube.drivers.s3_test.driver import S3TestDriver
    assert S3TestDriver is not None
