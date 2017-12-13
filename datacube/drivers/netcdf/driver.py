"""NetCDF storage driver class
"""
from __future__ import absolute_import

from datacube.drivers.driver import Driver
from datacube.storage.storage import write_dataset_to_netcdf, RasterDatasetSource
from datacube.drivers.netcdf.index import Index


class NetCDFDriver(Driver):
    """NetCDF storage driver. A placeholder for now.
    """

    @property
    def format(self):
        """Output format for this driver for use in metadata."""
        return 'NetCDF'

    @property
    def uri_scheme(self):
        """URI scheme used by this driver."""
        return 'file'

    def write_dataset_to_storage(self, dataset, *args, **kargs):
        """See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        """
        return write_dataset_to_netcdf(dataset, *args, **kargs)

    def _init_index(self, driver_manager, index, *args, **kargs):
        """See :meth:`datacube.drivers.driver.init_index`"""
        return Index(index, *args, **kargs)

    def get_datasource(self, dataset, measurement_id):
        """See :meth:`datacube.drivers.driver.get_datasource`"""
        return RasterDatasetSource(dataset, measurement_id)
