from __future__ import absolute_import
from datacube.storage.storage import write_dataset_to_netcdf


class NetcdfWriterDriver(object):
    def __init__(self):
        pass

    @property
    def aliases(self):
        return ['NetCDF CF']

    @property
    def format(self):
        return 'NetCDF'

    def write_dataset_to_storage(self, dataset, filename,
                                 global_attributes=None,
                                 variable_params=None,
                                 storage_config=None,
                                 **kwargs):
        # TODO: Currently ingestor copies chunking info from storage_config to
        # variable_params, this logic should probably happen here.

        write_dataset_to_netcdf(dataset, filename,
                                global_attributes=global_attributes,
                                variable_params=variable_params,
                                **kwargs)

        return {}


def writer_driver_init():
    return NetcdfWriterDriver()
