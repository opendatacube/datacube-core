from __future__ import print_function, absolute_import

from datacube.drivers.loader import DriverLoader
from datacube.drivers.netcdf.driver import NetCDFDriver
from datacube.drivers.s3.driver import S3Driver


def test_load_drivers():
    '''Simple check that the current 2 (s3 and netcdf) drivers are
    loaded.
    '''
    drivers = DriverLoader().drivers
    assert len(drivers) == 2

    driver_names = [driver.name for driver in drivers.values()]
    assert sorted(driver_names) == sorted([NetCDFDriver().name, S3Driver().name])
