'''NetCDF storage driver class
'''
from __future__ import absolute_import

from datacube.drivers.driver import Driver
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.drivers.s3.index import Index


class NetCDFDriver(Driver):
    '''NetCDF storage driver. A placeholder for now.
    '''

    @property
    def format(self):
        '''Output format for this driver for use in metadata.'''
        return 'NetCDF'


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        '''
        return write_dataset_to_netcdf(dataset, *args, **kargs)


    def _init_index(self, local_config=None, application_name=None, validate_connection=True):
        '''See :meth:`datacube.drivers.driver.init_index`'''
        return Index(local_config, application_name, validate_connection)
