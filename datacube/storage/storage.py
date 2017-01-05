# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
from contextlib import contextmanager
from pathlib import Path

from datacube.model import CRS
from datacube.storage import netcdf_writer
from datacube.config import OPTIONS
from datacube.utils import clamp, datetime_to_seconds_since_1970, is_url, uri_to_local_path
from datacube.compat import urlparse, urljoin

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import numpy

from affine import Affine
from datacube.compat import integer_types
import rasterio.warp
import rasterio.crs

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
        return str(src.crs.wkt)
else:
    def _rasterio_crs_wkt(src):
        return str(src.crs_wkt)

if str(rasterio.__version__) >= '1.0':
    def _rasterio_transform(src):
        return src.transform
else:
    def _rasterio_transform(src):
        return src.affine


def _calc_offsets(off, src_size, dst_size):
    """
    >>> _calc_offsets(11, 10, 12) # no overlap
    (10, 0, 0)
    >>> _calc_offsets(-11, 12, 10) # no overlap
    (0, 10, 0)
    >>> _calc_offsets(5, 10, 12) # overlap
    (5, 0, 5)
    >>> _calc_offsets(-5, 12, 10) # overlap
    (0, 5, 5)
    >>> _calc_offsets(5, 10, 4) # containment
    (5, 0, 4)
    >>> _calc_offsets(-5, 4, 10) # containment
    (0, 5, 4)
    """
    read_off = clamp(off, 0, src_size)
    write_off = clamp(-off, 0, dst_size)
    size = min(src_size - read_off, dst_size - write_off)
    return read_off, write_off, size


def _no_scale(affine, eps=0.01):
    return abs(affine.a - 1.0) < eps and abs(affine.e - 1.0) < eps


def _no_fractional_translate(affine, eps=0.01):
    return abs(affine.c % 1.0) < eps and abs(affine.f % 1.0) < eps


def read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, resampling):
    """
    Read from `source` into `dest`, reprojecting if necessary.

    :param BaseRasterDataSource source: Data source
    :param numpy.ndarray dest: Data destination
    """
    with source.open() as src:
        array_transform = ~src.transform * dst_transform
        if (src.crs == dst_projection and _no_scale(array_transform) and
                (resampling == Resampling.nearest or _no_fractional_translate(array_transform))):
            dy_dx = int(round(array_transform.f)), int(round(array_transform.c))
            read, write, shape = zip(*map(_calc_offsets, dy_dx, src.shape, dest.shape))

            dest.fill(dst_nodata)
            if all(shape):
                window = ((read[0], read[0] + shape[0]), (read[1], read[1] + shape[1]))
                tmp = src.read(window=window)
                numpy.copyto(dest[write[0]:write[0] + shape[0], write[1]:write[1] + shape[1]],
                             tmp, where=(tmp != src.nodata))
        else:
            if dest.dtype == numpy.dtype('int8'):
                dest = dest.view(dtype='uint8')
                dst_nodata = dst_nodata.astype('uint8')

            src.reproject(dest,
                          dst_transform=dst_transform,
                          dst_crs=str(dst_projection),
                          dst_nodata=dst_nodata,
                          resampling=resampling,
                          NUM_THREADS=OPTIONS['reproject_threads'])


@contextmanager
def ignore_if(ignore_errors):
    """Ignore Exceptions raised within this block if ignore_errors is True"""
    if ignore_errors:
        try:
            yield
        except OSError as e:
            _LOG.warning('Ignoring Exception: %s', e)
    else:
        yield


def reproject_and_fuse(sources, destination, dst_transform, dst_projection, dst_nodata,
                       resampling='nearest', fuse_func=None, skip_broken_datasets=False):
    """
    Reproject and fuse `sources` into a 2D numpy array `destination`.

    :param List[BaseRasterDataSource] sources: Data sources to open and read from
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
        with ignore_if(skip_broken_datasets):
            read_from_source(sources[0], destination, dst_transform, dst_nodata, dst_projection, resampling)
        return destination
    else:
        # Muitiple sources, we need to fuse them together into a single array
        buffer_ = numpy.empty(destination.shape, dtype=destination.dtype)
        for source in sources:
            with ignore_if(skip_broken_datasets):
                read_from_source(source, buffer_, dst_transform, dst_nodata, dst_projection, resampling)
                fuse_func(destination, buffer_)

        return destination


class BandDataSource(object):
    def __init__(self, source, nodata=None):
        self.source = source
        if nodata is None:
            assert self.source.ds.nodatavals[0] is not None
            nodata = self.dtype.type(self.source.ds.nodatavals[0])
        self.nodata = nodata

    @property
    def crs(self):
        return CRS(_rasterio_crs_wkt(self.source.ds))

    @property
    def transform(self):
        return _rasterio_transform(self.source.ds)

    @property
    def dtype(self):
        return numpy.dtype(self.source.dtype)

    @property
    def shape(self):
        return self.source.shape

    def read(self, window=None):
        return self.source.ds.read(indexes=self.source.bidx, window=window)

    def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
        return rasterio.warp.reproject(self.source,
                                       dest,
                                       src_nodata=self.nodata,
                                       dst_transform=dst_transform,
                                       dst_crs=str(dst_crs),
                                       dst_nodata=dst_nodata,
                                       resampling=resampling,
                                       **kwargs)


class OverrideBandDataSource(object):
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

    def read(self, window=None):
        return self.source.ds.read(indexes=self.source.bidx, window=window)

    def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
        source = self.read(self.source)  # TODO: read only the part the we care about
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


class BaseRasterDataSource(object):
    """
    Interface used by fuse_sources and read_from_source
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
        """Context manager which returns a `BandDataSource`"""
        try:
            _LOG.debug("opening %s", self.filename)
            with rasterio.open(self.filename) as src:
                override = False

                transform = _rasterio_transform(src)
                if transform.is_identity:
                    override = True
                    transform = self.get_transform(src.shape)

                try:
                    crs = CRS(_rasterio_crs_wkt(src))
                except ValueError:
                    override = True
                    crs = self.get_crs()

                bandnumber = self.get_bandnumber(src)
                band = rasterio.band(src, bandnumber)
                nodata = numpy.dtype(band.dtype).type(src.nodatavals[0] if src.nodatavals[0] is not None
                                                      else self.nodata)

                if override:
                    yield OverrideBandDataSource(band, nodata=nodata, crs=crs, transform=transform)
                else:
                    yield BandDataSource(band, nodata=nodata)

        except Exception as e:
            _LOG.error("Error opening source dataset: %s", self.filename)
            raise e


class RasterFileDataSource(BaseRasterDataSource):
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


class DatasetSource(BaseRasterDataSource):
    """Data source for reading from a Datacube Dataset"""
    def __init__(self, dataset, measurement_id):
        self._dataset = dataset
        self._measurement = dataset.measurements[measurement_id]
        url = _resolve_url(dataset.local_uri, self._measurement['path'])
        filename = _url2rasterio(url, dataset.format, self._measurement.get('layer'))
        nodata = dataset.type.measurements[measurement_id].get('nodata')
        super(DatasetSource, self).__init__(filename, nodata=nodata)

    def get_bandnumber(self, src):
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
        bounds = self._dataset.bounds
        width = bounds.right - bounds.left
        height = bounds.top - bounds.bottom
        return (Affine.translation(bounds.left, bounds.bottom) *
                Affine.scale(width / shape[1], height / shape[0]))

    def get_crs(self):
        return self._dataset.crs


def create_netcdf_storage_unit(filename,
                               crs, coordinates, variables, variable_params, global_attributes=None,
                               netcdfparams=None):
    """
    Create a NetCDF file on disk.

    :param pathlib.Path filename: filename to write to
    :param datacube.model.CRS crs: Datacube CRS object defining the spatial projection
    :param dict coordinates: Dict of named `datacube.model.Coordinate`s to create
    :param dict variables: Dict of named `datacube.model.Variable`s to create
    :param dict variable_params:
        Dict of dicts, with keys matching variable names, of extra parameters for variables
    :param dict global_attributes: named global attributes to add to output file
    :param dict netcdfparams: Extra parameters to use when creating netcdf file
    :return: open netCDF4.Dataset object, ready for writing to
    """
    filename = Path(filename)
    if filename.exists():
        raise RuntimeError('Storage Unit already exists: %s' % filename)

    try:
        filename.parent.mkdir(parents=True)
    except OSError:
        pass

    _LOG.info('Creating storage unit: %s', filename)

    nco = netcdf_writer.create_netcdf(str(filename), **(netcdfparams or {}))

    for name, coord in coordinates.items():
        netcdf_writer.create_coordinate(nco, name, coord.values, coord.units)

    netcdf_writer.create_grid_mapping_variable(nco, crs)

    for name, variable in variables.items():
        set_crs = all(dim in variable.dims for dim in crs.dimensions)
        var_params = variable_params.get(name, {})
        data_var = netcdf_writer.create_variable(nco, name, variable, set_crs=set_crs, **var_params)

        for key, value in var_params.get('attrs', {}).items():
            setattr(data_var, key, value)

    for key, value in (global_attributes or {}).items():
        setattr(nco, key, value)

    return nco


def write_dataset_to_netcdf(dataset, filename, global_attributes=None, variable_params=None,
                            netcdfparams=None):
    """
    Write a Data Cube style xarray Dataset to a NetCDF file

    Requires a spatial Dataset, with attached coordinates and global crs attribute.

    :param `xarray.Dataset` dataset:
    :param filename: Output filename
    :param global_attributes: Global file attributes. dict of attr_name: attr_value
    :param variable_params: dict of variable_name: {param_name: param_value, [...]}
                            Allows setting storage and compression options per variable.
                            See the `netCDF4.Dataset.createVariable` for available
                            parameters.
    :param netcdfparams: Optional params affecting netCDF file creation
    """
    global_attributes = global_attributes or {}
    variable_params = variable_params or {}
    filename = Path(filename)

    nco = create_netcdf_storage_unit(filename,
                                     dataset.crs,
                                     dataset.coords,
                                     dataset.data_vars,
                                     variable_params,
                                     global_attributes,
                                     netcdfparams)

    for name, variable in dataset.data_vars.items():
        nco[name][:] = netcdf_writer.netcdfy_data(variable.values)

    nco.close()
