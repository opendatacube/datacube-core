""" Example reader plugin
"""
from contextlib import contextmanager
import pickle
import rasterio.warp

from datacube.storage.storage import measurement_paths

PROTOCOL = 'file'
FORMAT = 'pickle'


def uri_split(uri):
    loc = uri.find('://')
    if loc < 0:
        return uri, PROTOCOL
    return uri[loc+3:], uri[:loc]


class PickleDataSource(object):
    class BandDataSource(object):
        def __init__(self, da):
            self._da = da
            self.nodata = da.nodata

        @property
        def crs(self):
            return self._da.crs

        @property
        def transform(self):
            return self._da.affine

        @property
        def dtype(self):
            return self._da.dtype

        @property
        def shape(self):
            return self._da.shape

        def read(self, window=None, out_shape=None):
            if window is None:
                data = self._da.values
            else:
                rows, cols = [slice(*w) for w in window]
                data = self._da.values[rows, cols]

            if out_shape is None or out_shape == data.shape:
                return data

            raise NotImplementedError('Native reading not supported for this data source')

        def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
            source = self.read()
            return rasterio.warp.reproject(source,
                                           dest,
                                           src_transform=self.transform,
                                           src_crs=str(self.crs),
                                           src_nodata=self.nodata,
                                           dst_transform=dst_transform,
                                           dst_crs=str(dst_crs),
                                           dst_nodata=dst_nodata,
                                           resampling=resampling,
                                           **kwargs)

    def __init__(self, dataset, band_name):
        self._band_name = band_name
        uri = measurement_paths(dataset)[band_name]
        self._filename, protocol = uri_split(uri)

        if protocol not in [PROTOCOL, 'pickle']:
            raise ValueError('Expected file:// or pickle:// url')

    @contextmanager
    def open(self):
        with open(self._filename, 'rb') as f:
            ds = pickle.load(f)

        yield PickleDataSource.BandDataSource(ds[self._band_name].isel(time=0))


class PickleReaderDriver(object):
    def __init__(self):
        self.name = 'PickleReader'
        self.protocols = [PROTOCOL, 'pickle']
        self.formats = [FORMAT]

    def supports(self, protocol, fmt):
        return (protocol in self.protocols and
                fmt in self.formats)

    def new_datasource(self, dataset, band_name):
        return PickleDataSource(dataset, band_name)


def rdr_driver_init():
    return PickleReaderDriver()


class PickleWriterDriver(object):
    def __init__(self):
        pass

    @property
    def aliases(self):
        return ['pickles']

    @property
    def format(self):
        return FORMAT

    @property
    def uri_scheme(self):
        return PROTOCOL

    def write_dataset_to_storage(self, dataset, filename,
                                 global_attributes=None,
                                 variable_params=None,
                                 storage_config=None,
                                 **kwargs):
        with open(filename, 'wb') as f:
            pickle.dump(dataset, f)
        return {}


def writer_driver_init():
    return PickleWriterDriver()
