from __future__ import print_function, absolute_import
import pytest

# s3_reqs_available = True
# try:
#     import SharedArray
# except ImportError:
#     s3_reqs_available = False




def test_can_import_drivermanager():
    from datacube.drivers.manager import DriverManager


def test_can_import_netcdfdriver():
    from datacube.drivers.netcdf.driver import NetCDFDriver


def test_can_import_s3_driver():
    pytest.importorskip('SharedArray')
    from datacube.drivers.s3.driver import S3Driver


def test_can_import_s3testdriver():
    pytest.importorskip('SharedArray')
    from datacube.drivers.s3_test.driver import S3TestDriver
