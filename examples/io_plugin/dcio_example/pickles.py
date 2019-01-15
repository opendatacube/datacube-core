""" Example reader plugin
"""
from contextlib import contextmanager
import pickle


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

    def __init__(self, band):
        self._band = band
        uri = band.uri
        self._filename, protocol = uri_split(uri)

        if protocol not in [PROTOCOL, 'pickle']:
            raise ValueError('Expected file:// or pickle:// url')

    @contextmanager
    def open(self):
        with open(self._filename, 'rb') as f:
            ds = pickle.load(f)

        yield PickleDataSource.BandDataSource(ds[self._band.name].isel(time=0))


class PickleReaderDriver(object):
    def __init__(self):
        self.name = 'PickleReader'
        self.protocols = [PROTOCOL, 'pickle']
        self.formats = [FORMAT]

    def supports(self, protocol, fmt):
        return (protocol in self.protocols and
                fmt in self.formats)

    def new_datasource(self, band):
        return PickleDataSource(band)


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
