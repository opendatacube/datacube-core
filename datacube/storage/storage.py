# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings.

Important functions are:

* :func:`reproject_and_fuse`
* :func:`read_from_source`

"""
from __future__ import absolute_import, division, print_function

import logging
from contextlib import contextmanager
from pathlib import Path

from datacube.compat import urlparse, urljoin, url_parse_module
from datacube.config import OPTIONS
from datacube.model import Dataset
from datacube.utils import datetime_to_seconds_since_1970, ignore_exceptions_if
from datacube.utils import geometry
from datacube.utils import is_url, uri_to_local_path

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import numpy

from affine import Affine
from datacube.compat import integer_types
import rasterio

try:
    from rasterio.warp import Resampling
except ImportError:
    from rasterio.warp import RESAMPLING as Resampling

_LOG = logging.getLogger(__name__)

RESAMPLING_METHODS = {
    'nearest': Resampling.nearest,
    'cubic': Resampling.cubic,
    'bilinear': Resampling.bilinear,
    'cubic_spline': Resampling.cubic_spline,
    'lanczos': Resampling.lanczos,
    'average': Resampling.average,
}

assert str(rasterio.__version__) >= '0.34.0', "rasterio version 0.34.0 or higher is required"
GDAL_NETCDF_DIM = ('NETCDF_DIM_'
                   if str(rasterio.__gdal_version__) >= '1.10.0' else
                   'NETCDF_DIMENSION_')


def _rasterio_resampling_method(resampling):
    return RESAMPLING_METHODS[resampling.lower()]


if str(rasterio.__version__) >= '0.36.0':
    def _rasterio_crs_wkt(src):
        if src.crs:
            return str(src.crs.wkt)
        else:
            return ''
else:
    def _rasterio_crs_wkt(src):
        return str(src.crs_wkt)

if str(rasterio.__version__) >= '1.0':
    def _rasterio_transform(src):
        return src.transform
else:
    def _rasterio_transform(src):
        return src.affine


def reproject_and_fuse(sources, destination, dst_transform, dst_projection, dst_nodata,
                       resampling='nearest', fuse_func=None, skip_broken_datasets=False):
    """
    Reproject and fuse `sources` into a 2D numpy array `destination`.

    :param List[RasterioDataSource] sources: Data sources to open and read from
    :param numpy.ndarray destination: ndarray of appropriate size to read data into
    :type resampling: str
    :type fuse_func: callable or None
    :param bool skip_broken_datasets: Carry on in the face of adversity and failing reads.
    """
    assert len(destination.shape) == 2

    resampling = _rasterio_resampling_method(resampling)

    def copyto_fuser(dest, src):
        """
        :type dest: numpy.ndarray
        :type src: numpy.ndarray
        """
        numpy.copyto(dest, src, where=(dest == dst_nodata))

    fuse_func = fuse_func or copyto_fuser

    destination.fill(dst_nodata)
    if len(sources) == 0:
        return destination
    elif len(sources) == 1:
        with ignore_exceptions_if(skip_broken_datasets):
            read_from_source(sources[0], destination, dst_transform, dst_nodata, dst_projection, resampling)
        return destination
    else:
        # Muitiple sources, we need to fuse them together into a single array
        buffer_ = numpy.empty(destination.shape, dtype=destination.dtype)
        for source in sources:
            with ignore_exceptions_if(skip_broken_datasets):
                read_from_source(source, buffer_, dst_transform, dst_nodata, dst_projection, resampling)
                fuse_func(destination, buffer_)

        return destination


def read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, resampling):
    """
    Read from `source` into `dest`, reprojecting if necessary.

    :param BaseRasterioSource source: Data source
    :param numpy.ndarray dest: Data destination
    """
    with source.open() as src:
        if dest.dtype == numpy.dtype('int8'):
            dest = dest.view(dtype='uint8')
            dst_nodata = dst_nodata.astype('uint8')

        # A bug in rasterio or GDAL causes data reads to fail if only a single row is requested and num_threads > 1
        num_threads = OPTIONS['reproject_threads']
        if dest.shape[0] == 1:
            num_threads = 1

        src.reproject(dest,
                      dst_transform=dst_transform,
                      dst_crs=str(dst_projection),
                      dst_nodata=dst_nodata,
                      resampling=resampling,
                      num_threads=num_threads)


class RasterioBandSource(object):
    """
    Wrapper for a :class:`rasterio.Band` object

    :type source: rasterio.Band
    """

    def __init__(self, source, nodata=None):
        self.source = source
        if nodata is None:
            assert self.source.ds.nodatavals[0] is not None
            nodata = self.dtype.type(self.source.ds.nodatavals[0])
        self.nodata = nodata

    @property
    def crs(self):
        return geometry.CRS(_rasterio_crs_wkt(self.source.ds))

    @property
    def transform(self):
        return _rasterio_transform(self.source.ds)

    @property
    def dtype(self):
        return numpy.dtype(self.source.dtype)

    @property
    def shape(self):
        return self.source.shape

    def read(self, window=None, out_shape=None):
        """Read data in the native format, returning a numpy array
        """
        return self.source.ds.read(indexes=self.source.bidx, window=window, out_shape=out_shape)

    def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
        """
        Read from `self.source` and store into `dest`, reprojecting if necessary.

        :param np.ndarray dest: Data is read or reprojected and stored in this array
        """
        rasterio.warp.reproject(self.source,
                                dest,
                                src_nodata=self.nodata,
                                dst_transform=dst_transform,
                                dst_crs=str(dst_crs),
                                dst_nodata=dst_nodata,
                                resampling=resampling,
                                **kwargs)

    def __repr__(self):
        return "BandDataSource(source={!r},...)".format(self.source)

    def __str__(self):
        return self.__repr__()


# class NetCDFDataSource(object):
#     def __init__(self, dataset, variable, slab=None, nodata=None):
#         self.dataset = dataset
#         self.variable = self.dataset[variable]
#         self.slab = slab or {}
#         if nodata is None:
#             nodata = self.variable.getncattr('_FillValue')
#         self.nodata = nodata
#
#     @property
#     def crs(self):
#         crs_var_name = self.variable.grid_mapping
#         crs_var = self.dataset[crs_var_name]
#         return geometry.CRS(crs_var.crs_wkt)
#
#     @property
#     def transform(self):
#         dims = self.crs.dimensions
#         xres, xoff = data_resolution_and_offset(self.dataset[dims[1]])
#         yres, yoff = data_resolution_and_offset(self.dataset[dims[0]])
#         return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)
#
#     @property
#     def dtype(self):
#         return self.variable.dtype
#
#     @property
#     def shape(self):
#         return self.variable.shape
#
#     def read(self, window=None, out_shape=None):
#         data = self.variable
#         if window is None:
#             window = ((0, data.shape[0]), (0, data.shape[1]))
#         data_shape = (window[0][1]-window[0][0]), (window[1][1]-window[1][0])
#         if out_shape is None:
#             out_shape = data_shape
#         xidx = window[0][0] + ((
# numpy.arange(out_shape[1])+0.5)*(data_shape[1]/out_shape[1])-0.5).round().astype('int')
#         yidx = window[1][0] + ((
# numpy.arange(out_shape[0])+0.5)*(data_shape[0]/out_shape[0])-0.5).round().astype('int')
#         slab = {self.crs.dimensions[1]: xidx, self.crs.dimensions[0]: yidx}
#         slab.update(self.slab)
#         return data[tuple(slab[d] for d in self.variable.dimensions)]
#
#     def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
#         dst_poly = geometry.polygon_from_transform(dest.shape[1], dest.shape[0],
#                                                    dst_transform, dst_crs).to_crs(self.crs)
#         src_poly = geometry.polygon_from_transform(self.shape[1], self.shape[0],
#                                                    self.transform, self.crs)
#         bounds = dst_poly.intersection(src_poly)
#         geobox = geometry.GeoBox.from_geopolygon(bounds, (self.transform.e, self.transform.a), crs=self.crs)
#         tmp, _, tmp_transform = _read_decimated(~self.transform * geobox.affine, self, geobox.shape)
#
#         return rasterio.warp.reproject(tmp,
#                                        dest,
#                                        src_transform=self.transform * tmp_transform,
#                                        src_crs=str(geobox.crs),
#                                        src_nodata=self.nodata,
#                                        dst_transform=dst_transform,
#                                        dst_crs=str(dst_crs),
#                                        dst_nodata=dst_nodata,
#                                        resampling=resampling,
#                                        **kwargs)


class OverrideBandSource(object):
    """
    Wrapper for a :class:`rasterio.Band` object that overrides `nodata`, `crs` and `transform`

    This is useful for files with malformed or missing properties, eg. the poorly supported
    BoM rainfall data stored in NetCDF on the NCI.

    :type source: rasterio.Band
    """

    def __init__(self, source, nodata, crs, transform):
        self.source = source
        self.nodata = nodata
        self.crs = crs
        self.transform = transform

    @property
    def dtype(self):
        return numpy.dtype(self.source.dtype)

    @property
    def shape(self):
        return self.source.shape

    def read(self, window=None, out_shape=None):
        """Read data in the native format, returning a native array
        """
        return self.source.ds.read(indexes=self.source.bidx, window=window, out_shape=out_shape)

    def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
        source = self.read()  # TODO: read only the part the we care about
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


class BaseRasterioSource(object):
    """
    Abstract class used by :func:`read_from_source`

    Concrete implementations available in :class:`RasterFileDataSource` and
    :class:`RasterDatasetSource`.
    """

    def __init__(self, filename, nodata):
        self.filename = filename
        self.nodata = nodata

    def get_bandnumber(self, src):
        raise NotImplementedError()

    def get_transform(self, shape):
        raise NotImplementedError()

    def get_crs(self):
        raise NotImplementedError()

    @contextmanager
    def open(self):
        """Context manager which returns a :class:`BandDataSource`"""
        try:
            _LOG.debug("opening %s", self.filename)
            with rasterio.open(self.filename) as src:
                override = False

                transform = _rasterio_transform(src)
                if transform.is_identity:
                    override = True
                    transform = self.get_transform(src.shape)

                try:
                    crs = geometry.CRS(_rasterio_crs_wkt(src))
                except ValueError:
                    override = True
                    crs = self.get_crs()

                # The 1.0 onwards release of rasterio has a bug that means it
                # cannot read multiband data into a numpy array during reprojection
                # We override it here to force the reading and reprojection into separate steps
                # TODO: Remove when rasterio bug fixed
                bandnumber = self.get_bandnumber(src)
                if bandnumber > 1 and str(rasterio.__version__) >= '1.0':
                    override = True

                band = rasterio.band(src, bandnumber)
                nodata = numpy.dtype(band.dtype).type(src.nodatavals[0] if src.nodatavals[0] is not None
                                                      else self.nodata)

                if override:
                    yield OverrideBandSource(band, nodata=nodata, crs=crs, transform=transform)
                else:
                    yield RasterioBandSource(band, nodata=nodata)

        except Exception as e:
            _LOG.error("Error opening source dataset: %s", self.filename)
            raise e


class RasterFileDataSource(BaseRasterioSource):
    def __init__(self, filename, bandnumber, nodata=None, crs=None, transform=None):
        super(RasterFileDataSource, self).__init__(filename, nodata)
        self.bandnumber = bandnumber
        self.crs = crs
        self.transform = transform

    def get_bandnumber(self, src):
        return self.bandnumber

    def get_transform(self, shape):
        if self.transform is None:
            raise RuntimeError('No transform in the data and no fallback')
        return self.transform

    def get_crs(self):
        if self.crs is None:
            raise RuntimeError('No CRS in the data and no fallback')
        return self.crs


class DatacubeRasterSource(BaseRasterioSource):
    """Data source for reading from a Data Cube Dataset"""

    def __init__(self, dataset, measurement_id):
        """
        Initialise for reading from a Data Cube Dataset.

        :param Dataset dataset: dataset to read from
        :param str measurement_id: measurement to read. a single 'band' or 'slice'
        """
        self._dataset = dataset
        self._measurement = dataset.measurements[measurement_id]
        url = _resolve_url(_choose_location(dataset), self._measurement['path'])
        filename = _url2rasterio(url, dataset.format, self._measurement.get('layer'))
        nodata = dataset.type.measurements[measurement_id].get('nodata')
        super(DatacubeRasterSource, self).__init__(filename, nodata=nodata)

    def get_bandnumber(self, src):

        # If `band` property is set to an integer it overrides any other logic
        band = self._measurement.get('band')
        if band is not None:
            if isinstance(band, integer_types):
                return band
            else:
                _LOG.warning('Expected "band" property to be of integer type')

        if 'netcdf' not in self._dataset.format.lower():
            layer_id = self._measurement.get('layer', 1)
            return layer_id if isinstance(layer_id, integer_types) else 1

        tag_name = GDAL_NETCDF_DIM + 'time'
        if tag_name not in src.tags(1):  # TODO: support time-less datasets properly
            return 1

        time = self._dataset.center_time
        sec_since_1970 = datetime_to_seconds_since_1970(time)

        idx = 0
        dist = float('+inf')
        for i in range(1, src.count + 1):
            v = float(src.tags(i)[tag_name])
            if abs(sec_since_1970 - v) < dist:
                idx = i
                dist = abs(sec_since_1970 - v)
        return idx

    def get_transform(self, shape):
        return self._dataset.transform * Affine.scale(1 / shape[1], 1 / shape[0])

    def get_crs(self):
        return self._dataset.crs

    def __repr__(self):
        return "DatasetSource(dataset={!r},measurement={!r})".format(self._dataset, self._measurement)

    def __str__(self):
        return self.__repr__()


def register_scheme(*schemes):
    """
    Register additional uri schemes as supporting relative offsets (etc), so that band/measurement paths can be
    calculated relative to the base uri.
    """
    url_parse_module.uses_netloc.extend(schemes)
    url_parse_module.uses_relative.extend(schemes)
    url_parse_module.uses_params.extend(schemes)


# Not recognised by python by default. Doctests below will fail without it.
register_scheme('s3')


def _resolve_url(base_url, path):
    """
    If path is a URL or an absolute path return URL
    If path is a relative path return base_url joined with path

    >>> _resolve_url('file:///foo/abc', 'bar')
    'file:///foo/bar'
    >>> _resolve_url('file:///foo/abc', 'file:///bar')
    'file:///bar'
    >>> _resolve_url('file:///foo/abc', None)
    'file:///foo/abc'
    >>> _resolve_url('file:///foo/abc', '/bar')
    'file:///bar'
    >>> _resolve_url('http://foo.com/abc/odc-metadata.yaml', 'band-5.tif')
    'http://foo.com/abc/band-5.tif'
    >>> _resolve_url('s3://foo.com/abc/odc-metadata.yaml', 'band-5.tif')
    's3://foo.com/abc/band-5.tif'
    >>> _resolve_url('s3://foo.com/abc/odc-metadata.yaml?something', 'band-5.tif')
    's3://foo.com/abc/band-5.tif'
    """
    if path:
        if is_url(path):
            url_str = path
        elif Path(path).is_absolute():
            url_str = Path(path).as_uri()
        else:
            url_str = urljoin(base_url, path)
    else:
        url_str = base_url
    return url_str


def _url2rasterio(url_str, fmt, layer):
    """
    turn URL into a string that could be passed to raterio.open
    """
    url = urlparse(url_str)
    assert url.scheme, "Expecting URL with scheme here"

    # if format is NETCDF of HDF need to pass NETCDF:path:band as filename to rasterio/GDAL
    for nasty_format in ('netcdf', 'hdf'):
        if nasty_format in fmt.lower():
            if url.scheme != 'file':
                raise RuntimeError("Can't access %s over %s" % (fmt, url.scheme))
            filename = '%s:%s:%s' % (fmt, uri_to_local_path(url_str), layer)
            return filename

    if url.scheme and url.scheme != 'file':
        return url_str

    # if local path strip scheme and other gunk
    return str(uri_to_local_path(url_str))


def _choose_location(dataset):
    # type: (Dataset) -> str

    # If there's a local (filesystem) URI, prefer it.
    local_uri = dataset.local_uri
    if local_uri:
        return local_uri

    uris = dataset.uris
    if not uris:
        # Location-less datasets should have been filtered already.
        raise RuntimeError("No recorded location for dataset {}".format(dataset))

    # Newest location first, use it.
    # We may want more nuanced selection in the future.
    return uris[0]
