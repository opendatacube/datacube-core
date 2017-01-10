# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import, division

import logging
import math
import warnings
from collections import namedtuple, OrderedDict, Sequence
from pathlib import Path

import numpy
from affine import Affine

from datacube.utils import parse_time, cached_property, uri_to_local_path, intersects, schema_validated, DocReader
from datacube.utils import geometry
from datacube.utils.geometry import CRS, BoundingBox

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('values', 'units'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dims', 'units'))
CellIndex = namedtuple('CellIndex', ('x', 'y'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}

SCHEMA_PATH = Path(__file__).parent / 'schema'


def _round_to_res(value, res, acc=0.1):
    """
    >>> _round_to_res(0.2, 1.0)
    1
    >>> _round_to_res(0.0, 1.0)
    0
    >>> _round_to_res(0.05, 1.0)
    0
    """
    res = abs(res)
    return int(math.ceil((value - 0.1 * res) / res))


class Dataset(object):
    """
    A Dataset. A container of metadata, and refers typically to a multi-band raster on disk.

    Most important parts are the metadata_doc and uri.

    :type type_: DatasetType
    :param dict metadata_doc: the document (typically a parsed json/yaml)
    :param str local_uri: A URI to access this dataset locally.
    """

    def __init__(self, type_, metadata_doc, local_uri, sources=None,
                 indexed_by=None, indexed_time=None, archived_time=None):
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

        #: :type: datetime.datetime
        self.indexed_time = indexed_time

        # When the dataset was archived. Null it not archived.
        #: :type: datetime.datetime
        self.archived_time = archived_time

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
        bounds = self.metadata.grid_spatial['geo_ref_points']
        return BoundingBox(left=bounds['ul']['x'], right=bounds['lr']['x'],
                           top=bounds['ul']['y'], bottom=bounds['lr']['y'])

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
        :rtype: geometry.Geometry
        """

        def xytuple(obj):
            return obj['x'], obj['y']

        projection = self.metadata.grid_spatial

        if 'valid_data' in projection:
            return geometry.Geometry(projection['valid_data'], crs=self.crs)
        else:
            geo_ref_points = projection['geo_ref_points']
            return geometry.polygon([xytuple(geo_ref_points[key]) for key in ('ll', 'ul', 'ur', 'lr', 'll')],
                                    crs=self.crs)

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


def GeoPolygon(coordinates, crs):  # pylint: disable=invalid-name
    warnings.warn("GeoPolygon is depricated. Use datacube.utils.geometry.polygon", DeprecationWarning)
    if not isinstance(coordinates, Sequence):
        raise ValueError("points ({}) must be a sequence of (x, y) coordinates".format(coordinates))
    return geometry.polygon(coordinates + [coordinates[0]], crs=crs)


def _polygon_from_boundingbox(boundingbox, crs=None):
    points = [
        (boundingbox.left, boundingbox.top),
        (boundingbox.right, boundingbox.top),
        (boundingbox.right, boundingbox.bottom),
        (boundingbox.left, boundingbox.bottom),
        (boundingbox.left, boundingbox.top),
    ]
    return geometry.polygon(points, crs=crs)
GeoPolygon.from_boundingbox = _polygon_from_boundingbox


def _polygon_from_sources_extents(sources, geobox):
    sources_union = geometry.unary_union(source.extent.to_crs(geobox.crs) for source in sources)
    valid_data = geobox.extent.intersection(sources_union)
    return valid_data
GeoPolygon.from_sources_extents = _polygon_from_sources_extents


class FlagsDefinition(object):
    def __init__(self, flags_def_dict):
        self.flags_def_dict = flags_def_dict


class SpectralDefinition(object):
    def __init__(self, spec_def_dict):
        self.spec_def_dict = spec_def_dict


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
        Tile size in pixels in CRS dimension order (Usually y,x or lat,lon)

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
            return (index + (1 if resolution < 0 < size else 0)) * size + origin

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
        geobox = GeoBox(crs=self.crs, affine=Affine(res_x, 0.0, x, 0.0, res_y, y), width=w, height=h)
        return geobox

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

    def tiles_inside_geopolygon(self, geopolygon, tile_buffer=(0, 0)):
        """
        Returns an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid and inside the specified `polygon`.

        .. note::

           Grid cells are referenced by coordinates `(x, y)`, which is the opposite to the usual CRS
           dimension order.

        :param geometry.Geometry geopolygon: Polygon to tile
        :param tile_buffer:
        :return: iterator of grid cells with :py:class:`GeoBox` tiles
        """
        result = []
        geopolygon = geopolygon.to_crs(self.crs)
        for tile_index, tile_geobox in self.tiles(geopolygon.boundingbox.buffered(*tile_buffer)):
            if tile_buffer:
                tile_geobox = tile_geobox.buffered(*tile_buffer)

            if intersects(tile_geobox.extent, geopolygon):
                result.append((tile_index, tile_geobox))
        return result

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

        points = [(0, 0), (0, height), (width, height), (width, 0), (0, 0)]
        self.affine.itransform(points)
        #: :rtype: geometry.Geometry
        self.extent = geometry.polygon(points, crs=crs)

    @classmethod
    def from_geopolygon(cls, geopolygon, resolution, crs=None, align=None):
        """
        :type geopolygon: geometry.Geometry
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

    def buffered(self, ybuff, xbuff):
        """
        Produce a tile buffered by ybuff, xbuff (in CRS units)
        """
        w, h = (_round_to_res(buf, res) for buf, res in zip((ybuff, xbuff), self.resolution))
        return self[-h:self.height+h, -w:self.width+w]

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
        :rtype: geometry.Geometry
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
