from contextlib import contextmanager
import pickle

from datacube.storage.storage import measurement_paths


def uri_split(uri):
    loc = uri.find('://')
    if loc < 0:
        return uri, 'file'
    return uri[loc+3:], uri[:loc]


class PickleDataSource(object):
    class BandDataSource(object):
        def __init__(self, da):
            self._da = da
            self.nodata = da.nodata
            pass

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

            raise NotImplementedError('Decimated reading not supported for this data source')

        def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
            pass

    def __init__(self, dataset, band_name):
        self._band_name = band_name
        uri = measurement_paths(dataset)[band_name]
        self._filename, protocol = uri_split(uri)

        if protocol not in ['file', 'pickle']:
            raise ValueError('Expected file:// or pickle:// url')

    @contextmanager
    def open(self):
        with open(self._filename, 'rb') as f:
            ds = pickle.load(f)

        yield PickleDataSource.BandDataSource(ds[self._band_name].isel(time=0))


class PickleReaderDriver(object):
    def __init__(self):
        self.name = 'PickleReader'
        self.protocols = ['file', 'pickle']
        self.formats = ['pickle']

    def supports(self, protocol, format):
        return (protocol in self.protocols and
                format in self.formats)

    def new_datasource(self, dataset, band_name):
        return PickleDataSource(dataset, band_name)


def init_driver():
    return PickleReaderDriver()
