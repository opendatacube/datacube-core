'''NetCDF storage driver class
'''
from __future__ import absolute_import

from datacube.drivers.driver import Driver
from datacube.storage.storage import write_dataset_to_netcdf, RasterDatasetSource
from datacube.drivers.netcdf.index import Index


class NetCDFDriver(Driver):
    '''NetCDF storage driver. A placeholder for now.
    '''

    @property
    def format(self):
        '''Output format for this driver for use in metadata.'''
        return 'NetCDF'


    @property
    def uri_scheme(self):
        '''URI scheme used by this driver.'''
        return 'file'


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        '''
        return write_dataset_to_netcdf(dataset, *args, **kargs)


    def _init_index(self, db=None, *args, **kargs):
        '''See :meth:`datacube.drivers.driver.init_index`'''
        local_config = kargs['local_config'] if 'local_config' in kargs else None
        application_name = kargs['application_name'] if 'application_name' in kargs else None
        validate_connection = kargs['validate_connection'] if 'validate_connection' in kargs else True
        return Index(local_config, application_name, validate_connection, db)


    def get_datasource(self, dataset, measurement_id):
        '''See :meth:`datacube.drivers.driver.get_datasource`'''
        return RasterDatasetSource(dataset, measurement_id)
