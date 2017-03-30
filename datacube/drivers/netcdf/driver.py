'''NetCDF storage driver class
'''
from __future__ import absolute_import

from datacube.drivers.driver import Driver
from datacube.storage.storage import write_dataset_to_netcdf

class NetCDFDriver(Driver):
    '''NetCDF storage driver. A placeholder for now.
    '''

    @property
    def name(self):
        '''See :meth:`datacube.drivers.driver.name`
        '''
        return 'NetCDF CF'


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        '''
        return write_dataset_to_netcdf(dataset, *args, **kargs)
