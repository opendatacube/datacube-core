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
from uuid import UUID

from affine import Affine

from datacube.utils import geometry
from datacube.utils import parse_time, cached_property, uri_to_local_path, intersects, schema_validated, DocReader
from datacube.utils.geometry import (CRS as _CRS,
                                     GeoBox as _GeoBox,
                                     Coordinate as _Coordinate,
                                     BoundingBox as _BoundingBox)

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dims', 'units'))
CellIndex = namedtuple('CellIndex', ('x', 'y'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}

SCHEMA_PATH = Path(__file__).parent / 'schema'


class CRS(_CRS):
    def __init__(self, *args, **kwargs):
        warnings.warn("The 'CRS' class has been renamed to 'datacube.utils.geometry.CRS' and will be "
                      "removed from 'datacube.model'. Please update your code.",
                      DeprecationWarning)
        super(CRS, self).__init__(*args, **kwargs)


class GeoBox(_GeoBox):
    def __init__(self, *args, **kwargs):
        warnings.warn("The 'GeoBox' class has been renamed to 'datacube.utils.geometry.GeoBox' and will be "
                      "removed from 'datacube.model'. Please update your code.",
                      DeprecationWarning)
        super(GeoBox, self).__init__(*args, **kwargs)


class Coordinate(_Coordinate):
    def __init__(self, *args, **kwargs):
        warnings.warn("The 'Coordinate' class has been renamed to 'datacube.utils.geometry.Coordinate' and will be "
                      "removed from 'datacube.model'. Please update your code.",
                      DeprecationWarning)
        super(Coordinate, self).__init__(*args, **kwargs)


class BoundingBox(_BoundingBox):  # pylint: disable=duplicate-bases
    def __init__(self, *args, **kwargs):
        warnings.warn("The 'BoundingBox' class has been renamed to 'datacube.utils.geometry.BoundingBox' and will be "
                      "removed from 'datacube.model'. Please update your code.",
                      DeprecationWarning)
        super(BoundingBox, self).__init__(*args, **kwargs)


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

        #: The most recent local file available to access the data, if any.
        #: Note that a dataset can have multiple uris, not all of which are local files.
        #: To get all uris for a dataset, use index.datasets.get_locations()
        #: :type: str
        self.local_uri = local_uri

        #: The datasets that this dataset is derived from (if requested on load).
        #: :type: dict[str, Dataset]
        self.sources = sources

        if sources is not None:
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
        :rtype: UUID
        """
        # This is a string in a raw document.
        return UUID(self.metadata.id)

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
        :rtype: geometry.BoundingBox
        """
        bounds = self.metadata.grid_spatial['geo_ref_points']
        return geometry.BoundingBox(left=min(bounds['ur']['x'], bounds['ll']['x']),
                                    right=max(bounds['ur']['x'], bounds['ll']['x']),
                                    top=max(bounds['ur']['y'], bounds['ll']['y']),
                                    bottom=min(bounds['ur']['y'], bounds['ll']['y']))

    @property
    def transform(self):
        bounds = self.metadata.grid_spatial['geo_ref_points']
        return Affine(bounds['lr']['x'] - bounds['ul']['x'], 0, bounds['ul']['x'],
                      0, bounds['lr']['y'] - bounds['ul']['y'], bounds['ul']['y'])

    @property
    def is_archived(self):
        """
        Is this dataset archived?

        (an archived dataset is one that is not intended to be used by users anymore: eg. it has been
        replaced by another dataset. It will not show up in search results, but still exists in the
        system via provenance chains or through id lookup.)

        :rtype: bool
        """
        return self.archived_time is not None

    @property
    def crs(self):
        """
        :rtype: geometry.CRS
        """
        projection = self.metadata.grid_spatial
        if not projection:
            return None

        crs = projection.get('spatial_reference', None)
        if crs:
            return geometry.CRS(str(crs))

        # Try to infer CRS
        zone_ = projection.get('zone')
        datum_ = projection.get('datum')
        if zone_ and datum_:
            try:
                # TODO: really need CRS specified properly in agdc-metadata.yaml
                if datum_ == 'GDA94':
                    return geometry.CRS('EPSG:283' + str(abs(zone_)))
                if datum_ == 'WGS84':
                    if zone_[-1] == 'S':
                        return geometry.CRS('EPSG:327' + str(abs(int(zone_[:-1]))))
                    else:
                        return geometry.CRS('EPSG:326' + str(abs(int(zone_[:-1]))))
            except geometry.InvalidCRSError:
                # We still return None, as they didn't specify a CRS explicitly...
                _LOG.warning(
                    "Can't figure out projection: possibly invalid zone (%r) for datum (%r).", zone_, datum_
                )

        return None

    @cached_property
    def extent(self):
        """
        :rtype: geometry.Geometry
        """

        def xytuple(obj):
            return obj['x'], obj['y']

        # If no projection or crs, they have no extent.
        projection = self.metadata.grid_spatial
        if not projection:
            return None
        crs = self.crs
        if not crs:
            _LOG.debug("No CRS, assuming no extent (dataset %s)", self.id)
            return None

        valid_data = projection.get('valid_data')
        geo_ref_points = projection.get('geo_ref_points')
        if valid_data:
            return geometry.Geometry(valid_data, crs=crs)
        elif geo_ref_points:
            return geometry.polygon([xytuple(geo_ref_points[key]) for key in ('ll', 'ul', 'ur', 'lr', 'll')],
                                    crs=crs)

        return None

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

        #: :type: dict[str,datacube.index.fields.Field]
        self.dataset_fields = dataset_search_fields

        #: :type: int
        self.id = id_

    @property
    def name(self):
        return self.definition['name']

    @property
    def description(self):
        return self.definition['description']

    def dataset_reader(self, dataset_doc):
        return DocReader(self.definition['dataset'], self.dataset_fields, dataset_doc)

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
        crs = geometry.CRS(str(storage['crs']).strip())

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
    warnings.warn("GeoPolygon is depricated. Use 'datacube.utils.geometry.polygon'", DeprecationWarning)
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

    >>> gs = GridSpec(crs=geometry.CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.1, 0.1), origin=(-50.05, 139.95))
    >>> gs.tile_resolution
    (10, 10)
    >>> list(gs.tiles(geometry.BoundingBox(140, -50, 141.5, -48.5)))
    [((0, 0), GeoBox(10, 10, Affine(0.1, 0.0, 139.95,
           0.0, -0.1, -49.05), EPSG:4326)), ((1, 0), GeoBox(10, 10, Affine(0.1, 0.0, 140.95,
           0.0, -0.1, -49.05), EPSG:4326)), ((0, 1), GeoBox(10, 10, Affine(0.1, 0.0, 139.95,
           0.0, -0.1, -48.05), EPSG:4326)), ((1, 1), GeoBox(10, 10, Affine(0.1, 0.0, 140.95,
           0.0, -0.1, -48.05), EPSG:4326))]

    :param geometry.CRS crs: Coordinate System used to define the grid
    :param [float,float] tile_size: (Y, X) size of each tile, in CRS units
    :param [float,float] resolution: (Y, X) size of each data point in the grid, in CRS units. Y will
                                   usually be negative.
    :param [float,float] origin: (Y, X) coordinates of a corner of the (0,0) tile in CRS units. default is (0.0, 0.0)
    """

    def __init__(self, crs, tile_size, resolution, origin=None):
        #: :rtype: geometry.CRS
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
        :rtype: datacube.utils.geometry.GeoBox
        """
        res_y, res_x = self.resolution
        y, x = self.tile_coords(tile_index)
        h, w = self.tile_resolution
        geobox = geometry.GeoBox(crs=self.crs, affine=Affine(res_x, 0.0, x, 0.0, res_y, y), width=w, height=h)
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
