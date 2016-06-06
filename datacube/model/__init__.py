# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import, division

import logging
import math
from collections import namedtuple, OrderedDict

import numpy
import os
from affine import Affine
from osgeo import osr
from pathlib import Path
from rasterio.coords import BoundingBox

from datacube import compat
from datacube.compat import parse_url
from datacube.utils import get_doc_offset, parse_time, grid_range, read_documents, validate_document

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('labels', 'units'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dimensions', 'units'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}

SCHEMA_PATH = Path(__file__).parent/'schema'


def _uri_to_local_path(local_uri):
    """
    Platform dependent URI to Path
    :type local_uri: str
    :rtype: pathlib.Path

    For example on Unix:
    'file:///tmp/something.txt' -> '/tmp/something.txt'

    On Windows:
    'file:///C:/tmp/something.txt' -> 'C:\\tmp\\test.tmp'

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

    """
    def __init__(self, type_, metadata_doc, local_uri, sources=None):
        """
        A dataset on disk.

        :type type_: DatasetType
        :param metadata_doc: the document (typically a parsed json/yaml)
        :type metadata_doc: dict
        :param local_uri: A URI to access this dataset locally.
        :type local_uri: str
        """
        #: :type: DatasetType
        self.type = type_

        #: :type: dict
        self.metadata_doc = metadata_doc

        #: :type: str
        self.local_uri = local_uri

        #: :type: dict[str, Dataset]
        self.sources = sources or {}

        assert set(self.metadata.sources.keys()) == set(self.sources.keys())

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
        return self.metadata.measurements

    @property
    def center_time(self):
        """
        :type: datetime.datetime
        """
        time = self.time
        return time.begin + (time.end - time.begin)//2

    @property
    def time(self):
        time = self.metadata.time
        return Range(parse_time(time.begin), parse_time(time.begin))

    @property
    def bounds(self):
        return self.extent.boundingbox

    @property
    def crs(self):
        """
        :type: datacube.model.CRS
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
    def validate(cls, document):
        return validate_document(document, cls.schema)

    def decorate(cls):
        cls.schema = next(iter(read_documents(SCHEMA_PATH/schema)))[1]
        cls.validate = classmethod(validate)
        return cls

    return decorate


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
    """
    def __init__(self,
                 metadata_type,
                 definition,
                 id_=None):
        """
        DatasetType of datasets & storage.

        :type metadata_type: MetadataType
        """
        self.validate(definition)

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
    def metadata(self):
        return self.definition['metadata']

    @property
    def fields(self):
        return self.metadata_type.dataset_reader(self.metadata).fields

    @property
    def measurements(self):
        return OrderedDict((m['name'], m) for m in self.definition.get('measurements', []))

    @property
    def dimensions(self):
        assert self.metadata_type.name == 'eo'
        return ('time', ) + self.grid_spec.dimensions

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


class GeoPolygon(object):
    """
    Polygon with a :py:class:`CRS`
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
        Copy polygon to another CRS

        :return: new GeoPolygon with CRS specified by crs
        :rtype: GeoPolygon
        """
        if self.crs == crs:
            return self

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs) # pylint: disable=protected-access
        return GeoPolygon([p[:2] for p in transform.TransformPoints(self.points)], crs)

    def __str__(self):
        return "GeoPolygon(points=%s, crs=%s)" % (self.points, self.crs)

    def __repr__(self):
        return self.__str__()


class FlagsDefinition(object):
    pass


class CRSProjProxy(object):
    def __init__(self, crs):
        self._crs = crs

    def __getattr__(self, item):
        return self._crs.GetProjParm(item)


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
    """
    def __init__(self, crs_str):
        self.crs_str = crs_str
        self._crs = osr.SpatialReference()
        self._crs.SetFromUserInput(crs_str)

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
        return self.__str__()

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


    """
    def __init__(self, crs=None, tile_size=None, resolution=None):
        """
        Create a Grid Specification

        :type crs: CRS
        :param tile_size: Size of each area of the grid, in CRS units
        :type tile_size: tuple(x, y)
        :param resolution: Size of each pixel in the grid, in CRS units
        """
        self.crs = crs
        self.tile_size = tile_size
        self.resolution = resolution

    @property
    def dimensions(self):
        return self.crs.dimensions

    @property
    def tile_resolution(self):
        return [int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution)]

    def tiles(self, bounds):
        """
        Return an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid.

        :param bounds: Boundary coordinates of the required grid
        :return: iterator across geoboxes of tiles in a grid
        """
        grid_size = self.tile_size
        for y in grid_range(bounds.bottom, bounds.top, grid_size[1]):
            for x in grid_range(bounds.left, bounds.right, grid_size[0]):
                tile_index = (x, y)
                yield tile_index, GeoBox.from_grid_spec(self, tile_index)

    def __str__(self):
        return "GridSpec(crs=%s, tile_size=%s, resolution=%s)" % (self.crs, self.tile_size, self.resolution)

    def __repr__(self):
        return self.__str__()


class GeoBox(object):
    """
    Defines the location and resolution of a rectangular grid of data,
    including it's :py:class:`CRS`.

    >>> from affine import Affine
    >>> t = GeoBox(4000, 4000, Affine(0.00025, 0.0, 151.0, 0.0, -0.00025, -29.0), CRS('EPSG:4326'))
    >>> t.coordinates['latitude'].labels
    array([-29.000125, -29.000375, -29.000625, ..., -29.999375, -29.999625,
           -29.999875])
    >>> t.coordinates['longitude'].labels
    array([ 151.000125,  151.000375,  151.000625, ...,  151.999375,
            151.999625,  151.999875])
    >>> t.geographic_extent.points
    [(151.0, -29.0), (151.0, -30.0), (152.0, -30.0), (152.0, -29.0)]


    :param crs: Coordinate Reference System
    :type crs: CRS
    :param affine: Affine transformation defining the location of the storage unit
    :type affine: affine.Affine
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

        :type grid_spec:  datacube.model.GridSpec
        :type tile_index: tuple(int,int)
        :rtype: datacube.model.GeoBox
        """
        tile_size = grid_spec.tile_size
        tile_res = grid_spec.resolution

        x = (tile_index[0] + (1 if tile_res[1] < 0 else 0)) * tile_size[1]
        y = (tile_index[1] + (1 if tile_res[0] < 0 else 0)) * tile_size[0]

        return cls(crs=grid_spec.crs,
                   affine=Affine(tile_res[1], 0.0, x, 0.0, tile_res[0], y),
                   width=int(tile_size[1] / abs(tile_res[1])),
                   height=int(tile_size[0] / abs(tile_res[0])))

    @classmethod
    def from_geopolygon(cls, geopolygon, resolution, crs=None, align=True):
        """
        :type geopolygon: datacube.model.GeoPolygon
        :param resolution: (x_resolution, y_resolution)
        :param crs: CRS to use, if different from the geopolygon
        :param align: Should the geobox be aligned to pixels of the given resolution. This assumes an origin of (0,0).
        :type align: boolean
        :rtype: GeoBox
        """
        # TODO: currently only flipped Y-axis data is supported
        assert resolution[1] > 0
        assert resolution[0] < 0

        if crs is None:
            crs = geopolygon.crs
        else:
            geopolygon = geopolygon.to_crs(crs)

        bounding_box = geopolygon.boundingbox
        left, top = float(bounding_box.left), float(bounding_box.top)
        if align:
            left = math.floor(left / resolution[1]) * resolution[1]
            top = math.floor(top / resolution[0]) * resolution[0]
        affine = (Affine.translation(left, top) * Affine.scale(resolution[1], resolution[0]))
        right, bottom = float(bounding_box.right), float(bounding_box.bottom)
        width, height = ~affine * (right, bottom)
        if align:
            width = math.ceil(width)
            height = math.ceil(height)
        return GeoBox(crs=crs,
                      affine=affine,
                      width=int(width),
                      height=int(height))

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
