from datacube.storage._rio import RasterDatasetDataSource
from datacube.utils.uris import normalise_path
from ._write import write_dataset_to_netcdf

PROTOCOL = 'file'
FORMAT = 'NetCDF'


class NetcdfReaderDriver(object):
    def __init__(self):
        self.name = 'NetcdfReader'
        self.protocols = [PROTOCOL]
        self.formats = [FORMAT]

    def supports(self, protocol, fmt):
        return (protocol in self.protocols and
                fmt in self.formats)

    def new_datasource(self, band):
        return RasterDatasetDataSource(band)


def reader_driver_init():
    return NetcdfReaderDriver()


class NetcdfWriterDriver(object):
    def __init__(self):
        pass

    @property
    def aliases(self):
        return ['NetCDF CF']

    @property
    def format(self):
        return FORMAT

    @property
    def uri_scheme(self):
        return PROTOCOL

    def mk_uri(self, file_path, storage_config):
        """
        Constructs a uri from the file_path and storage config.
        resource.
        """
        driver_alias = storage_config['driver']
        if driver_alias in self.aliases:
            return normalise_path(file_path).as_uri()
        else:
            raise ValueError(f'Unknown driver alias: {driver_alias}')

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
