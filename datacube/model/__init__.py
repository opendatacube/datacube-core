# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import, division

import logging
import math
import os
from collections import namedtuple, OrderedDict

import numpy
import cachetools
from affine import Affine
from osgeo import osr
from pathlib import Path
from rasterio.coords import BoundingBox

from datacube import compat
from datacube.compat import parse_url
from datacube.utils import get_doc_offset, parse_time, read_documents, validate_document, cached_property

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('values', 'units'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dims', 'units'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}

SCHEMA_PATH = Path(__file__).parent / 'schema'


def _uri_to_local_path(local_uri):
    """
    Transforms a URI to a platform dependent Path
    :type local_uri: str
    :rtype: pathlib.Path

    For example on Unix:
    'file:///tmp/something.txt' -> '/tmp/something.txt'

    On Windows:
    'file:///C:/tmp/something.txt' -> 'C:\\tmp\\test.tmp'

    .. note:
        Only supports file:// schema URIs
    """
    if not local_uri:
        return None

    components = parse_url(local_uri)
    if components.scheme != 'file':
        raise ValueError('Only file URIs currently supported. Tried %r.' % components.scheme)

    path = _cross_platform_path(components.path)

    return Path(path)


def _cross_platform_path(path):
    if os.name == 'nt':
        return path[1:]
    else:
        return path


class Dataset(object):
    """
    A Dataset stored on disk

    :type type_: DatasetType
    :param dict metadata_doc: the document (typically a parsed json/yaml)
    :param str local_uri: A URI to access this dataset locally.
    """

    def __init__(self, type_, metadata_doc, local_uri, sources=None, indexed_by=None, indexed_time=None):
        #: :type: DatasetType
        self.type = type_

        #: :type: dict
        self.metadata_doc = metadata_doc

        #: :type: str
        self.local_uri = local_uri

        #: :type: dict[str, Dataset]
        self.sources = sources or {}

        assert set(self.metadata.sources.keys()) == set(self.sources.keys())

        # User who indexed this dataset
        #: :type: str
        self.indexed_by = indexed_by
        self.indexed_time = indexed_time

    @property
    def metadata_type(self):
        return self.type.metadata_type if self.type else None

    @property
    def local_path(self):
        """
        A path to this dataset on the local filesystem (if available).

        :rtype: pathlib.Path
        """
        return _uri_to_local_path(self.local_uri)

    @property
    def id(self):
        return self.metadata.id

    @property
    def managed(self):
        return self.type.managed

    @property
    def format(self):
        return self.metadata.format

    @property
    def measurements(self):
        # It's an optional field in documents.
        # Dictionary of key -> measurement descriptor
        if not hasattr(self.metadata, 'measurements'):
            return {}
        return self.metadata.measurements

    @cached_property
    def center_time(self):
        """
        :rtype: datetime.datetime
        """
        time = self.time
        return time.begin + (time.end - time.begin) // 2

    @property
    def time(self):
        time = self.metadata.time
        return Range(parse_time(time.begin), parse_time(time.end))

    @property
    def bounds(self):
        return self.extent.boundingbox

    @property
    def crs(self):
        """
        :rtype: datacube.model.CRS
        """
        projection = self.metadata.grid_spatial

        crs = projection.get('spatial_reference', None)
        if crs:
            return CRS(str(crs))

        # TODO: really need CRS specified properly in agdc-metadata.yaml
        if projection['datum'] == 'GDA94':
            return CRS('EPSG:283' + str(abs(projection['zone'])))

        if projection['datum'] == 'WGS84':
            if projection['zone'][-1] == 'S':
                return CRS('EPSG:327' + str(abs(int(projection['zone'][:-1]))))
            else:
                return CRS('EPSG:326' + str(abs(int(projection['zone'][:-1]))))

        raise RuntimeError('Cant figure out the projection: %s %s' % (projection['datum'], projection['zone']))

    @property
    def extent(self):
        def xytuple(obj):
            return obj['x'], obj['y']

        projection = self.metadata.grid_spatial

        if 'valid_data' in projection:
            assert projection['valid_data']['type'].lower() == 'polygon'
            return GeoPolygon(projection['valid_data']['coordinates'][0][:-1], crs=self.crs)
        else:
            geo_ref_points = projection['geo_ref_points']
            return GeoPolygon([xytuple(geo_ref_points[key]) for key in ('ll', 'ul', 'ur', 'lr')], crs=self.crs)

    def __str__(self):
        return "Dataset <id={id} type={type} location={loc}>".format(id=self.id,
                                                                     type=self.type.name,
                                                                     loc=self.local_path)

    def __repr__(self):
        return self.__str__()

    @property
    def metadata(self):
        return self.metadata_type.dataset_reader(self.metadata_doc)


def schema_validated(schema):
    """
    Decorates a class to enable validating it's definition against a JSON Schema file.

    Adds a self.validate() method which takes a dict used to populate the instantiated class.

    :param str schema: filename of the json schema, relative to `SCHEMA_PATH`
    :return: wrapped class
    """

    def validate(cls, document):
        return validate_document(document, cls.schema)

    def decorate(cls):
        cls.schema = next(iter(read_documents(SCHEMA_PATH / schema)))[1]
        cls.validate = classmethod(validate)
        return cls

    return decorate


class Measurement(object):
    def __init__(self, measurement_dict):
        self.name = measurement_dict['name']
        self.dtype = measurement_dict['dtype']
        self.nodata = measurement_dict['nodata']
        self.units = measurement_dict['units']
        self.aliases = measurement_dict['aliases']
        self.spectral_definition = measurement_dict['spectral_definition']
        self.flags_definition = measurement_dict['flags_definition']


@schema_validated('metadata-type-schema.yaml')
class MetadataType(object):
    def __init__(self,
                 name,
                 dataset_offsets,
                 dataset_search_fields,
                 id_=None):
        self.name = name
        #: :type: DatasetOffsets
        self.dataset_offsets = dataset_offsets

        #: :type: dict[str, datacube.index.fields.Field]
        self.dataset_fields = dataset_search_fields

        self.id = id_

    def dataset_reader(self, dataset_doc):
        return _DocReader(self.dataset_offsets, self.dataset_fields, dataset_doc)

    def __str__(self):
        return "MetadataType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)


@schema_validated('dataset-type-schema.yaml')
class DatasetType(object):
    """
    Definition of a Dataset

    :type metadata_type: MetadataType
    """

    def __init__(self,
                 metadata_type,
                 definition,
                 id_=None):
        self.id = id_

        # All datasets in a collection must have the same metadata_type.
        self.metadata_type = metadata_type

        # DatasetType definition.
        self.definition = definition

    @property
    def name(self):
        return self.definition['name']

    @property
    def managed(self):
        return self.definition.get('managed', False)

    @property
    def metadata_doc(self):
        return self.definition['metadata']

    @property
    def metadata(self):
        return self.metadata_type.dataset_reader(self.metadata_doc)

    @property
    def fields(self):
        return self.metadata_type.dataset_reader(self.metadata_doc).fields

    @property
    def measurements(self):
        return OrderedDict((m['name'], m) for m in self.definition.get('measurements', []))

    @property
    def dimensions(self):
        assert self.metadata_type.name == 'eo'
        return ('time',) + self.grid_spec.dimensions

    @property
    def grid_spec(self):
        if 'storage' not in self.definition:
            return None
        storage = self.definition['storage']

        if 'crs' not in storage:
            return None
        crs = CRS(str(storage['crs']).strip())

        tile_size = None
        if 'tile_size' in storage:
            tile_size = [storage['tile_size'][dim] for dim in crs.dimensions]

        resolution = None
        if 'resolution' in storage:
            resolution = [storage['resolution'][dim] for dim in crs.dimensions]

        return GridSpec(crs=crs, tile_size=tile_size, resolution=resolution)

    def dataset_reader(self, dataset_doc):
        return self.metadata_type.dataset_reader(dataset_doc)

    def __str__(self):
        return "DatasetType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self):
        return self.__str__()

    # Types are uniquely identifiable by name:

    def __eq__(self, other):
        if self is other:
            return True

        if self.__class__ != other.__class__:
            return False

        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class GeoPolygon(object):
    """
    Polygon with a :py:class:`CRS`

    :param points: list of (x,y) points
    :param CRS crs: coordinate system for the polygon
    """

    def __init__(self, points, crs=None):
        self.points = points
        self.crs = crs

    @classmethod
    def from_boundingbox(cls, boundingbox, crs=None):
        points = [
            (boundingbox.left, boundingbox.top),
            (boundingbox.right, boundingbox.top),
            (boundingbox.right, boundingbox.bottom),
            (boundingbox.left, boundingbox.bottom),
        ]
        return cls(points, crs)

    @property
    def boundingbox(self):
        return BoundingBox(left=min(x for x, y in self.points),
                           bottom=min(y for x, y in self.points),
                           right=max(x for x, y in self.points),
                           top=max(y for x, y in self.points))

    def to_crs(self, crs):
        """
        Duplicates polygon while transforming to a new CRS

        :param CRS crs: Target CRS
        :return: new GeoPolygon with CRS specified by crs
        :rtype: GeoPolygon
        """
        if self.crs == crs:
            return self

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs)  # pylint: disable=protected-access
        return GeoPolygon([p[:2] for p in transform.TransformPoints(self.points)], crs)

    def __str__(self):
        return "GeoPolygon(points=%s, crs=%s)" % (self.points, self.crs)

    def __repr__(self):
        return self.__str__()


class FlagsDefinition(object):
    def __init__(self, flags_def_dict):
        self.flags_def_dict = flags_def_dict

        # blue_saturated:
        #   bits: 0
        #   description: Blue band is saturated
        #   values: {0: true, 1: false}
        # green_saturated:
        #   bits: 1
        #   description: Green band is saturated
        #   values: {0: true, 1: false}
        # red_saturated:
        #   bits: 2
        #   description: Red band is saturated
        #   values: {0: true, 1: false}
        # nir_saturated:
        #   bits: 3
        #   description: NIR band is saturated
        #   values: {0: true, 1: false}


class SpectralDefinition(object):
    def __init__(self, spec_def_dict):
        self.spec_def_dict = spec_def_dict


class CRSProjProxy(object):
    def __init__(self, crs):
        self._crs = crs

    def __getattr__(self, item):
        return self._crs.GetProjParm(item)


@cachetools.cached({})
def _make_crs(crs_str):
    crs = osr.SpatialReference()
    crs.SetFromUserInput(crs_str)
    if not crs.ExportToProj4() or crs.IsGeographic() == crs.IsProjected():
        raise ValueError('Not a valid CRS: %s' % crs_str)
    return crs


class CRS(object):
    """
    Wrapper around `osr.SpatialReference` providing a more pythonic interface

    >>> crs = CRS('EPSG:3577')
    >>> crs.geographic
    False
    >>> crs.projected
    True
    >>> crs.dimensions
    ('y', 'x')
    >>> crs = CRS('EPSG:4326')
    >>> crs.geographic
    True
    >>> crs.projected
    False
    >>> crs.dimensions
    ('latitude', 'longitude')
    >>> CRS('EPSG:3577') == CRS('EPSG:3577')
    True
    >>> CRS('EPSG:3577') == CRS('EPSG:4326')
    False
    >>> CRS('blah')
    Traceback (most recent call last):
        ...
    ValueError: Not a valid CRS: blah
    >>> CRS('PROJCS["unnamed",\
    ... GEOGCS["WGS 84", DATUM["WGS_1984", SPHEROID["WGS 84",6378137,298.257223563, AUTHORITY["EPSG","7030"]],\
    ... AUTHORITY["EPSG","6326"]], PRIMEM["Greenwich",0, AUTHORITY["EPSG","8901"]],\
    ... UNIT["degree",0.0174532925199433, AUTHORITY["EPSG","9122"]], AUTHORITY["EPSG","4326"]]]')
    Traceback (most recent call last):
        ...
    ValueError: Not a valid CRS: ...
    """

    def __init__(self, crs_str):
        if isinstance(crs_str, CRS):
            crs_str = crs_str.crs_str
        self.crs_str = crs_str
        self._crs = _make_crs(crs_str)

    def __getitem__(self, item):
        return self._crs.GetAttrValue(item)

    def __getstate__(self):
        return {'crs_str': self.crs_str}

    def __setstate__(self, state):
        self.__init__(state['crs_str'])

    @property
    def wkt(self):
        return self._crs.ExportToWkt()

    @property
    def proj(self):
        return CRSProjProxy(self._crs)

    @property
    def semi_major_axis(self):
        return self._crs.GetSemiMajor()

    @property
    def semi_minor_axis(self):
        return self._crs.GetSemiMinor()

    @property
    def inverse_flattening(self):
        return self._crs.GetInvFlattening()

    @property
    def geographic(self):
        return self._crs.IsGeographic() == 1

    @property
    def projected(self):
        return self._crs.IsProjected() == 1

    @property
    def dimensions(self):
        if self.geographic:
            return 'latitude', 'longitude'

        if self.projected:
            return 'y', 'x'

    def __str__(self):
        return self.crs_str

    def __repr__(self):
        return "CRS('%s')" % self.crs_str

    def __eq__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        return self._crs.IsSame(other._crs) == 1  # pylint: disable=protected-access

    def __ne__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        return self._crs.IsSame(other._crs) != 1  # pylint: disable=protected-access


class GridSpec(object):
    """
    Definition for a regular spatial grid

    >>> gs = GridSpec(crs=CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.001, 0.001))
    >>> gs.tile_resolution
    (1000, 1000)
    >>> list(gs.tiles(BoundingBox(140, -50, 142, -48)))
    [((140, -50), GeoBox(1000, 1000, Affine(0.001, 0.0, 140.0,
           0.0, -0.001, -49.0), EPSG:4326)), ((141, -50), GeoBox(1000, 1000, Affine(0.001, 0.0, 141.0,
           0.0, -0.001, -49.0), EPSG:4326)), ((140, -49), GeoBox(1000, 1000, Affine(0.001, 0.0, 140.0,
           0.0, -0.001, -48.0), EPSG:4326)), ((141, -49), GeoBox(1000, 1000, Affine(0.001, 0.0, 141.0,
           0.0, -0.001, -48.0), EPSG:4326))]

    :param CRS crs: Coordinate System used to define the grid
    :param tuple(y, x) tile_size: Size of each tile, in CRS units
    :param tuple(y, x) resolution: Size of each data point in the grid, in CRS units. Y will
                                   usually be negative.
    """

    def __init__(self, crs=None, tile_size=None, resolution=None):
        self.crs = crs
        self.tile_size = tile_size
        self.resolution = resolution

    @property
    def dimensions(self):
        return self.crs.dimensions

    @property
    def tile_resolution(self):
        """
        Tile Resolution, or the size of each tile in pixels.

        Units will be in CRS dimension order (Usually y,x or lat,lon)

        :return: tuple()
        """
        return tuple(int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution))

    def tiles(self, bounds):
        """
        Returns an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid and inside the specified `bounds`.

        .. note::

           Grid cells are referenced by coordinates `(x, y)`, which is the opposite to the usual CRS
           dimension order.

        :param BoundingBox bounds: Boundary coordinates of the required grid
        :return: iterator of grid cells with :py:class:`GeoBox` tiles
        """
        tile_size_y, tile_size_x = self.tile_size
        for y in GridSpec.grid_range(bounds.bottom, bounds.top, tile_size_y):
            for x in GridSpec.grid_range(bounds.left, bounds.right, tile_size_x):
                tile_index = (x, y)
                yield tile_index, GeoBox.from_grid_spec(self, tile_index)

    @staticmethod
    def grid_range(lower, upper, step):
        """
        Returns the indices along a 1D scale.

        Used for producing 2D grid indices.

        >>> list(GridSpec.grid_range(-4.0, -1.0, 3.0))
        [-2, -1]
        >>> list(GridSpec.grid_range(-3.0, 0.0, 3.0))
        [-1]
        >>> list(GridSpec.grid_range(-2.0, 1.0, 3.0))
        [-1, 0]
        >>> list(GridSpec.grid_range(-1.0, 2.0, 3.0))
        [-1, 0]
        >>> list(GridSpec.grid_range(0.0, 3.0, 3.0))
        [0]
        >>> list(GridSpec.grid_range(1.0, 4.0, 3.0))
        [0, 1]
        """
        assert step > 0.0
        return range(int(math.floor(lower / step)), int(math.ceil(upper / step)))

    def __str__(self):
        return "GridSpec(crs=%s, tile_size=%s, resolution=%s)" % (
            self.crs, self.tile_size, self.resolution)

    def __repr__(self):
        return self.__str__()


class GeoBox(object):
    """
    Defines the location and resolution of a rectangular grid of data,
    including it's :py:class:`CRS`.

    >>> from affine import Affine
    >>> t = GeoBox(4000, 4000, Affine(0.00025, 0.0, 151.0, 0.0, -0.00025, -29.0), CRS('EPSG:4326'))
    >>> t.coordinates['latitude'].values
    array([-29.000125, -29.000375, -29.000625, ..., -29.999375, -29.999625,
           -29.999875])
    >>> t.coordinates['longitude'].values
    array([ 151.000125,  151.000375,  151.000625, ...,  151.999375,
            151.999625,  151.999875])
    >>> t.geographic_extent.points
    [(151.0, -29.0), (151.0, -30.0), (152.0, -30.0), (152.0, -29.0)]


    :param CRS crs: Coordinate Reference System
    :param affine.Affine affine: Affine transformation defining the location of the geobox
    """

    def __init__(self, width, height, affine, crs):
        assert height > 0 and width > 0
        self.width = width
        self.height = height
        self.affine = affine

        points = [(0, 0), (0, height), (width, height), (width, 0)]
        self.affine.itransform(points)
        self.extent = GeoPolygon(points, crs)

    @classmethod
    def from_grid_spec(cls, grid_spec, tile_index):
        """
        Returns the GeoBox for a tile index in the specified grid.

        :type grid_spec:  GridSpec
        :type tile_index: tuple(x, y)
        :rtype: GeoBox
        """
        tile_index_x, tile_index_y = tile_index
        tile_size_y, tile_size_x = grid_spec.tile_size
        tile_res_y, tile_res_x = grid_spec.resolution

        x = (tile_index_x + (1 if tile_res_x < 0 else 0)) * tile_size_x
        y = (tile_index_y + (1 if tile_res_y < 0 else 0)) * tile_size_y

        return cls(crs=grid_spec.crs,
                   affine=Affine(tile_res_x, 0.0, x, 0.0, tile_res_y, y),
                   width=int(tile_size_x / abs(tile_res_x)),
                   height=int(tile_size_y / abs(tile_res_y)))

    @classmethod
    def from_geopolygon(cls, geopolygon, resolution, crs=None, align=None):
        """
        :type geopolygon: GeoPolygon
        :param resolution: (x_resolution, y_resolution)
        :param CRS crs: CRS to use, if different from the geopolygon
        :param (float,float) align: Alight geobox such that point 'align' lies on the pixel boundary.
        :rtype: GeoBox
        """
        # TODO: currently only flipped Y-axis data is supported

        assert resolution[1] > 0
        assert resolution[0] < 0

        align = align or (0.0, 0.0)
        assert 0.0 <= align[1] <= abs(resolution[1])
        assert 0.0 <= align[0] <= abs(resolution[0])

        if crs is None:
            crs = geopolygon.crs
        else:
            geopolygon = geopolygon.to_crs(crs)

        def align_pix(val, res, off):
            return math.floor((val-off)/res) * res + off

        bounding_box = geopolygon.boundingbox
        left = align_pix(bounding_box.left, resolution[1], align[1])
        top = align_pix(bounding_box.top, resolution[0], align[0])
        affine = (Affine.translation(left, top) * Affine.scale(resolution[1], resolution[0]))
        return GeoBox(crs=crs,
                      affine=affine,
                      width=int(math.ceil((bounding_box.right-left)/resolution[1])),
                      height=int(math.ceil((bounding_box.bottom-top)/resolution[0])))

    def __getitem__(self, item):
        indexes = [slice(index.start or 0, index.stop or size, index.step or 1)
                   for size, index in zip(self.shape, item)]
        for index in indexes:
            if index.step != 1:
                raise NotImplementedError('scaling not implemented, yet')

        affine = self.affine * Affine.translation(indexes[1].start, indexes[0].start)
        return GeoBox(width=indexes[1].stop - indexes[1].start,
                      height=indexes[0].stop - indexes[0].start,
                      affine=affine,
                      crs=self.crs)

    @property
    def shape(self):
        return self.height, self.width

    @property
    def crs(self):
        return self.extent.crs

    @property
    def dimensions(self):
        return self.crs.dimensions

    @property
    def coordinates(self):
        xs = numpy.arange(self.width) * self.affine.a + self.affine.c + self.affine.a / 2
        ys = numpy.arange(self.height) * self.affine.e + self.affine.f + self.affine.e / 2

        if self.crs.geographic:
            return {
                'latitude': Coordinate(ys, 'degrees_north'),
                'longitude': Coordinate(xs, 'degrees_east')
            }

        elif self.crs.projected:
            units = self.crs['UNIT']
            return {
                'x': Coordinate(xs, units),
                'y': Coordinate(ys, units)
            }

    @property
    def geographic_extent(self):
        if self.crs.geographic:
            return self.extent
        return self.extent.to_crs(CRS('EPSG:4326'))

    def __str__(self):
        return "GeoBox({})".format(self.geographic_extent.points)

    def __repr__(self):
        return "GeoBox({width}, {height}, {affine!r}, {crs})".format(
            width=self.width,
            height=self.height,
            affine=self.affine,
            crs=self.extent.crs
        )


def _set_doc_offset(offset, document, value):
    """
    :type offset: list[str]
    :type document: dict

    >>> doc = {'a': 4}
    >>> _set_doc_offset(['a'], doc, 5)
    >>> doc
    {'a': 5}
    >>> doc = {'a': {'b': 4}}
    >>> _set_doc_offset(['a', 'b'], doc, 'c')
    >>> doc
    {'a': {'b': 'c'}}
    """
    read_offset = offset[:-1]
    sub_doc = get_doc_offset(read_offset, document)
    sub_doc[offset[-1]] = value


class _DocReader(object):
    def __init__(self, field_offsets, search_fields, doc):
        """
        :type field_offsets: dict[str,list[str]]
        :type doc: dict
        >>> d = _DocReader({'lat': ['extent', 'lat']}, {}, doc={'extent': {'lat': 4}})
        >>> d.lat
        4
        >>> d.lat = 5
        >>> d._doc
        {'extent': {'lat': 5}}
        >>> hasattr(d, 'lat')
        True
        >>> hasattr(d, 'lon')
        False
        >>> d.lon
        Traceback (most recent call last):
        ...
        AttributeError: Unknown field 'lon'. Expected one of ['lat']
        """
        self.__dict__['_doc'] = doc
        self.__dict__['_fields'] = {name: field for name, field in search_fields.items() if hasattr(field, 'extract')}
        self._fields.update(field_offsets)

    def __getattr__(self, name):
        field = self._fields.get(name)
        if field is None:
            raise AttributeError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._fields.keys())
                )
            )
        return self._unsafe_get_field(field)

    def __setattr__(self, name, val):
        offset = self._fields.get(name)
        if offset is None:
            raise AttributeError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._fields.keys())
                )
            )
        return _set_doc_offset(offset, self._doc, val)

    def _unsafe_get_field(self, field):
        if isinstance(field, list):
            return get_doc_offset(field, self._doc)
        else:
            return field.extract(self._doc)

    @property
    def fields(self):
        fields = {}
        for name, field in self._fields.items():
            try:
                fields[name] = self._unsafe_get_field(field)
            except (AttributeError, KeyError, ValueError):
                continue
        return fields
