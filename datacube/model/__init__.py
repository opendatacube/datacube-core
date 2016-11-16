# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import, division

import logging
import math
import collections
from collections import namedtuple, OrderedDict
from pathlib import Path

import numpy
import cachetools
from affine import Affine
from osgeo import osr
from rasterio.coords import BoundingBox

from datacube import compat
from datacube.utils import parse_time, cached_property, uri_to_local_path, check_intersect
from datacube.utils import schema_validated, DocReader, union_points, intersect_points, densify_points

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('values', 'units'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dims', 'units'))
CellIndex = namedtuple('CellIndex', ('x', 'y'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}

SCHEMA_PATH = Path(__file__).parent / 'schema'


class Dataset(object):
    """
    A Dataset. A container of metadata, and refers typically to a multiband raster on disk.

    Most important parts are the metadata_doc and uri.

    :type type_: DatasetType
    :param dict metadata_doc: the document (typically a parsed json/yaml)
    :param str local_uri: A URI to access this dataset locally.
    """

    def __init__(self, type_, metadata_doc, local_uri, sources=None, indexed_by=None, indexed_time=None):
        assert isinstance(type_, DatasetType)

        #: :rtype: DatasetType
        self.type = type_

        #: The document describing the dataset as a dictionary. It is often serialised as YAML on disk
        #: or inside a NetCDF file, and as JSON-B inside the database index.
        #: :type: dict
        self.metadata_doc = metadata_doc

        #: The local file or path that can be opened to access the raw data.
        #: :type: str
        self.local_uri = local_uri

        #: The datasets that this dataset is derived from.
        #: :type: dict[str, Dataset]
        self.sources = sources or {}

        assert set(self.metadata.sources.keys()) == set(self.sources.keys())

        #: The User who indexed this dataset
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
        return uri_to_local_path(self.local_uri)

    @property
    def id(self):
        """
        :rtype: uuid
        """
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
        """
        :rtype: rasterio.coords.BoundingBox
        """
        return self.extent.boundingbox

    @property
    def crs(self):
        """
        :rtype: CRS
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

    @cached_property
    def extent(self):
        """
        :rtype: GeoPolygon
        """

        def xytuple(obj):
            return obj['x'], obj['y']

        projection = self.metadata.grid_spatial

        if 'valid_data' in projection:
            assert projection['valid_data']['type'].lower() == 'polygon'
            return GeoPolygon(projection['valid_data']['coordinates'][0][:-1], crs=self.crs)
        else:
            geo_ref_points = projection['geo_ref_points']
            return GeoPolygon([xytuple(geo_ref_points[key]) for key in ('ll', 'ul', 'ur', 'lr')], crs=self.crs)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return "Dataset <id={id} type={type} location={loc}>".format(id=self.id,
                                                                     type=self.type.name,
                                                                     loc=self.local_path)

    def __repr__(self):
        return self.__str__()

    @property
    def metadata(self):
        return self.metadata_type.dataset_reader(self.metadata_doc)


class Measurement(object):
    def __init__(self, measurement_dict):
        self.name = measurement_dict['name']
        self.dtype = measurement_dict['dtype']
        self.nodata = measurement_dict['nodata']
        self.units = measurement_dict['units']
        self.aliases = measurement_dict['aliases']
        self.spectral_definition = measurement_dict['spectral_definition']
        self.flags_definition = measurement_dict['flags_definition']


@schema_validated(SCHEMA_PATH / 'metadata-type-schema.yaml')
class MetadataType(object):
    """Metadata Type definition"""

    def __init__(self,
                 definition,
                 dataset_search_fields,
                 id_=None):
        #: :type: dict
        self.definition = definition

        #: :rtype: dict[str,datacube.index.fields.Field]
        self.dataset_fields = dataset_search_fields

        #: :type: int
        self.id = id_

    @property
    def name(self):
        return self.definition['name']

    @property
    def description(self):
        return self.definition['description']

    @property
    def dataset_offsets(self):
        return self.definition['dataset']

    def dataset_reader(self, dataset_doc):
        return DocReader(self.dataset_offsets, self.dataset_fields, dataset_doc)

    def __str__(self):
        return "MetadataType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self):
        return str(self)


@schema_validated(SCHEMA_PATH / 'dataset-type-schema.yaml')
class DatasetType(object):
    """
    Product definition

    :param MetadataType metadata_type:
    :param dict definition:
    """

    def __init__(self,
                 metadata_type,
                 definition,
                 id_=None):
        assert isinstance(metadata_type, MetadataType)

        #: :type: int
        self.id = id_

        #: :rtype: MetadataType
        self.metadata_type = metadata_type

        #: product definition document
        self.definition = definition

    @property
    def name(self):
        """
        :type: str
        """
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
        """
        Dictionary of measurements in this product

        :type: dict[str, dict]
        """
        return OrderedDict((m['name'], m) for m in self.definition.get('measurements', []))

    @property
    def dimensions(self):
        """
        List of dimensions for data in this product

        :type: tuple[str]
        """
        assert self.metadata_type.name == 'eo'
        return ('time',) + self.grid_spec.dimensions

    @cached_property
    def grid_spec(self):
        """
        Grid specification for this product

        :rtype: GridSpec
        """
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

        origin = None
        if 'origin' in storage:
            origin = [storage['origin'][dim] for dim in crs.dimensions]

        return GridSpec(crs=crs, tile_size=tile_size, resolution=resolution, origin=origin)

    def lookup_measurements(self, measurements=None):
        """
        Find measurements by name

        :param list[str] measurements: list of measurement names
        :rtype: OrderedDict[str,dict]
        """
        my_measurements = self.measurements
        if measurements is None:
            return my_measurements
        return OrderedDict((measurement, my_measurements[measurement]) for measurement in measurements)

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

    @classmethod
    def from_geojson(cls, geojson_geometry, crs=None):
        try:
            assert isinstance(geojson_geometry['coordinates'], collections.Sequence)
            assert geojson_geometry['type'] == 'Polygon'
        except (KeyError, AssertionError):
            raise ValueError('Input geometry is not an acceptable geojson geometry. It should be of type Polygon,'
                             'and contain a single list of coordinates.')

        return cls(geojson_geometry['coordinates'][0], crs)

    @property
    def boundingbox(self):
        """
        :rtype: rasterio.coords.BoundingBox
        """
        return BoundingBox(left=min(x for x, y in self.points),
                           bottom=min(y for x, y in self.points),
                           right=max(x for x, y in self.points),
                           top=max(y for x, y in self.points))

    @classmethod
    def from_sources_extents(cls, sources, geobox):
        sources_union = union_points(*[source.extent.to_crs(geobox.crs).points for source in sources])
        valid_data = intersect_points(geobox.extent.points, sources_union)

        return cls(valid_data, geobox.crs)

    def to_crs(self, crs, resolution=None):
        """
        Duplicates polygon while transforming to a new CRS

        :param CRS crs: Target CRS
        :param resolution: resolution of points in source crs units to maintain in output polygon
        :return: new GeoPolygon with CRS specified by crs
        :rtype: GeoPolygon
        """
        if self.crs == crs:
            return self

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs)  # pylint: disable=protected-access
        return GeoPolygon([p[:2] for p in transform.TransformPoints(densify_points(self.points, resolution))], crs)

    def __str__(self):
        return "GeoPolygon(points=%s, crs=%s)" % (self.points, self.crs)

    def __repr__(self):
        return self.__str__()


class FlagsDefinition(object):
    def __init__(self, flags_def_dict):
        self.flags_def_dict = flags_def_dict


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
    >>> crs.epsg
    4326
    >>> crs.dimensions
    ('latitude', 'longitude')
    >>> crs = CRS('EPSG:3577')
    >>> crs.epsg
    3577
    >>> crs.dimensions
    ('y', 'x')
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
        """

        :param crs_str: string representation of a CRS, often an EPSG code like 'EPSG:4326'
        """
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
        """
        WKT representation of the CRS

        :type: str
        """
        return self._crs.ExportToWkt()

    @property
    def epsg(self):
        """
        EPSG Code of the CRS

        :type: int
        """
        if self.projected:
            return int(self._crs.GetAuthorityCode('PROJCS'))

        if self.geographic:
            return int(self._crs.GetAuthorityCode('GEOGCS'))

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
        """
        :type: bool
        """
        return self._crs.IsGeographic() == 1

    @property
    def projected(self):
        """
        :type: bool
        """
        return self._crs.IsProjected() == 1

    @property
    def dimensions(self):
        """
        List of dimension names of the CRS

        :type: (str,str)
        """
        if self.geographic:
            return 'latitude', 'longitude'

        if self.projected:
            return 'y', 'x'

    @property
    def units(self):
        """
        List of dimension units of the CRS

        :type: (str,str)
        """
        if self.geographic:
            return 'degrees_north', 'degrees_east'

        if self.projected:
            return self['UNIT'], self['UNIT']

    def __str__(self):
        return self.crs_str

    def __repr__(self):
        return "CRS('%s')" % self.crs_str

    def __eq__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        canonical = lambda crs: set(crs.ExportToProj4().split() + ['+wktext'])
        return canonical(self._crs) == canonical(other._crs)  # pylint: disable=protected-access

    def __ne__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        return self._crs.IsSame(other._crs) != 1  # pylint: disable=protected-access


class GridSpec(object):
    """
    Definition for a regular spatial grid

    >>> gs = GridSpec(crs=CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.1, 0.1), origin=(-50.05, 139.95))
    >>> gs.tile_resolution
    (10, 10)
    >>> list(gs.tiles(BoundingBox(140, -50, 141.5, -48.5)))
    [((0, 0), GeoBox(10, 10, Affine(0.1, 0.0, 139.95,
           0.0, -0.1, -49.05), EPSG:4326)), ((1, 0), GeoBox(10, 10, Affine(0.1, 0.0, 140.95,
           0.0, -0.1, -49.05), EPSG:4326)), ((0, 1), GeoBox(10, 10, Affine(0.1, 0.0, 139.95,
           0.0, -0.1, -48.05), EPSG:4326)), ((1, 1), GeoBox(10, 10, Affine(0.1, 0.0, 140.95,
           0.0, -0.1, -48.05), EPSG:4326))]

    :param CRS crs: Coordinate System used to define the grid
    :param [float,float] tile_size: (Y, X) size of each tile, in CRS units
    :param [float,float] resolution: (Y, X) size of each data point in the grid, in CRS units. Y will
                                   usually be negative.
    :param [float,float] origin: (Y, X) coordinates of a corner of the (0,0) tile in CRS units. default is (0.0, 0.0)
    """

    def __init__(self, crs, tile_size, resolution, origin=None):
        #: :rtype: CRS
        self.crs = crs
        #: :type: (float,float)
        self.tile_size = tile_size
        #: :type: (float,float)
        self.resolution = resolution
        #: :type: (float, float)
        self.origin = origin or (0.0, 0.0)

    @property
    def dimensions(self):
        """
        List of dimension names of the grid spec

        :type: (str,str)
        """
        return self.crs.dimensions

    @property
    def alignment(self):
        """
        Pixel boundary alignment

        :type: (float,float)
        """
        return tuple(orig % abs(res) for orig, res in zip(self.origin, self.resolution))

    @property
    def tile_resolution(self):
        """
        Tile size in pixels.

        Units will be in CRS dimension order (Usually y,x or lat,lon)

        :type: (float, float)
        """
        return tuple(int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution))

    def tile_coords(self, tile_index):
        """
        Tile coordinates in (Y,X) order

        :param (int,int) tile_index: in X,Y order
        :rtype: (float,float)
        """

        def coord(index, resolution, size, origin):
            return (index + (1 if resolution < 0 and size > 0 else 0)) * size + origin

        return tuple(coord(index, res, size, origin) for index, res, size, origin in
                     zip(tile_index[::-1], self.resolution, self.tile_size, self.origin))

    def tile_geobox(self, tile_index):
        """
        Tile geobox.

        :param (int,int) tile_index:
        :rtype: GeoBox
        """
        res_y, res_x = self.resolution
        y, x = self.tile_coords(tile_index)
        h, w = self.tile_resolution
        return GeoBox(crs=self.crs, affine=Affine(res_x, 0.0, x, 0.0, res_y, y), width=w, height=h)

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
        tile_origin_y, tile_origin_x = self.origin
        for y in GridSpec.grid_range(bounds.bottom - tile_origin_y, bounds.top - tile_origin_y, tile_size_y):
            for x in GridSpec.grid_range(bounds.left - tile_origin_x, bounds.right - tile_origin_x, tile_size_x):
                tile_index = (x, y)
                yield tile_index, self.tile_geobox(tile_index)

    def tiles_inside_geopolygon(self, geopolygon):
        """
        Returns an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid and inside the specified `polygon`.

        .. note::

           Grid cells are referenced by coordinates `(x, y)`, which is the opposite to the usual CRS
           dimension order.

        :param GeoPolygon geopolygon: Polygon to tile
        :return: iterator of grid cells with :py:class:`GeoBox` tiles
        """
        geopolygon = geopolygon.to_crs(self.crs)
        for tile_index, tile_geobox in self.tiles(geopolygon.boundingbox):
            if check_intersect(tile_geobox.extent, geopolygon):
                yield tile_index, tile_geobox

    @staticmethod
    def grid_range(lower, upper, step):
        """
        Returns the indices along a 1D scale.

        Used for producing 2D grid indices.

        >>> list(GridSpec.grid_range(-4.0, -1.0, 3.0))
        [-2, -1]
        >>> list(GridSpec.grid_range(1.0, 4.0, -3.0))
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
        if step < 0.0:
            lower, upper, step = -upper, -lower, -step
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
    >>> t.resolution
    (-0.00025, 0.00025)


    :param CRS crs: Coordinate Reference System
    :param affine.Affine affine: Affine transformation defining the location of the geobox
    """

    def __init__(self, width, height, affine, crs):
        assert height > 0 and width > 0, "Can't create GeoBox of zero size"
        #: :type: int
        self.width = width
        #: :type: int
        self.height = height
        #: :rtype: affine.Affine
        self.affine = affine

        points = [(0, 0), (0, height), (width, height), (width, 0)]
        self.affine.itransform(points)
        #: :rtype: GeoPolygon
        self.extent = GeoPolygon(points, crs)

    @classmethod
    def from_geopolygon(cls, geopolygon, resolution, crs=None, align=None):
        """
        :type geopolygon: GeoPolygon
        :param resolution: (y_resolution, x_resolution)
        :param CRS crs: CRS to use, if different from the geopolygon
        :param (float,float) align: Align geobox such that point 'align' lies on the pixel boundary.
        :rtype: GeoBox
        """
        # TODO: currently only flipped Y-axis data is supported

        assert resolution[1] > 0, "decreasing X coordinates are not supported"
        assert resolution[0] < 0, "increasing Y coordinates are not supported"

        align = align or (0.0, 0.0)
        assert 0.0 <= align[1] <= abs(resolution[1]), "X align must be in [0, abs(x_resolution)] range"
        assert 0.0 <= align[0] <= abs(resolution[0]), "Y align must be in [0, abs(y_resolution)] range"

        if crs is None:
            crs = geopolygon.crs
        else:
            geopolygon = geopolygon.to_crs(crs)

        def align_pix(val, res, off):
            return math.floor((val - off) / res) * res + off

        bounding_box = geopolygon.boundingbox
        left = align_pix(bounding_box.left, resolution[1], align[1])
        top = align_pix(bounding_box.top, resolution[0], align[0])
        affine = (Affine.translation(left, top) * Affine.scale(resolution[1], resolution[0]))
        return GeoBox(crs=crs,
                      affine=affine,
                      width=max(1, int(math.ceil((bounding_box.right - left - 0.1 * resolution[1]) / resolution[1]))),
                      height=max(1, int(math.ceil((bounding_box.bottom - top - 0.1 * resolution[0]) / resolution[0]))))

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
        """
        :type: (int,int)
        """
        return self.height, self.width

    @property
    def crs(self):
        """
        :rtype: CRS
        """
        return self.extent.crs

    @property
    def dimensions(self):
        """
        List of dimension names of the GeoBox

        :type: (str,str)
        """
        return self.crs.dimensions

    @property
    def resolution(self):
        """
        Resolution in Y,X dimensions

        :type: (float,float)
        """
        return self.affine.e, self.affine.a

    @property
    def alignment(self):
        """
        Alignment of pixel boundaries in Y,X dimensions

        :type: (float,float)
        """
        return self.affine.yoff % abs(self.affine.e), self.affine.xoff % abs(self.affine.a)

    @property
    def coordinates(self):
        """
        dict of coordinate labels

        :type: dict[str,numpy.array]
        """
        xs = numpy.arange(self.width) * self.affine.a + (self.affine.c + self.affine.a / 2)
        ys = numpy.arange(self.height) * self.affine.e + (self.affine.f + self.affine.e / 2)

        return OrderedDict((dim, Coordinate(labels, units)) for dim, labels, units in zip(self.crs.dimensions,
                                                                                          (ys, xs), self.crs.units))

    @property
    def geographic_extent(self):
        """
        :rtype: GeoPolygon
        """
        if self.crs.geographic:
            return self.extent
        return self.extent.to_crs(CRS('EPSG:4326'))

    coords = coordinates
    dims = dimensions

    def __str__(self):
        return "GeoBox({})".format(self.geographic_extent.points)

    def __repr__(self):
        return "GeoBox({width}, {height}, {affine!r}, {crs})".format(
            width=self.width,
            height=self.height,
            affine=self.affine,
            crs=self.extent.crs
        )
