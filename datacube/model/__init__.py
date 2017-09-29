# coding=utf-8
"""
Core classes used across modules.
"""
import logging
import math
import warnings
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from uuid import UUID

from affine import Affine
from typing import Optional, List, Mapping, Any, Dict, Tuple, Iterator

from urllib.parse import urlparse
from datacube.utils import geometry, without_lineage_sources, parse_time, cached_property, uri_to_local_path, \
    schema_validated, DocReader
from .fields import Field
from ._base import Range

_LOG = logging.getLogger(__name__)

DEFAULT_SPATIAL_DIMS = ('y', 'x')  # Used when product lacks grid_spec

SCHEMA_PATH = Path(__file__).parent / 'schema'


class Dataset(object):
    """
    A Dataset. A container of metadata, and refers typically to a multi-band raster on disk.

    Most important parts are the metadata_doc and uri.

    :param metadata_doc: the document (typically a parsed json/yaml)
    :param uris: All active uris for the dataset
    """

    def __init__(self,
                 type_: 'DatasetType',
                 metadata_doc: dict,
                 local_uri: Optional[str] = None,
                 uris: Optional[List[str]] = None,
                 sources: Optional[Mapping[str, 'Dataset']] = None,
                 indexed_by: Optional[str] = None,
                 indexed_time: Optional[datetime] = None,
                 archived_time: Optional[datetime] = None):
        assert isinstance(type_, DatasetType)

        self.type = type_

        #: The document describing the dataset as a dictionary. It is often serialised as YAML on disk
        #: or inside a NetCDF file, and as JSON-B inside the database index.
        self.metadata_doc = metadata_doc

        if local_uri:
            warnings.warn(
                "Dataset.local_uri has been replaced with list Dataset.uris",
                DeprecationWarning
            )
            if not uris:
                uris = []

            uris.append(local_uri)

        #: Active URIs in order from newest to oldest
        self.uris = uris

        #: The datasets that this dataset is derived from (if requested on load).
        self.sources = sources

        if self.sources is not None:
            assert set(self.metadata.sources.keys()) == set(self.sources.keys())

        #: The User who indexed this dataset
        self.indexed_by = indexed_by
        self.indexed_time = indexed_time
        # When the dataset was archived. Null it not archived.
        self.archived_time = archived_time

    @property
    def metadata_type(self) -> Optional['MetadataType']:
        return self.type.metadata_type if self.type else None

    @property
    def local_uri(self) -> Optional[str]:
        """
        The latest local file uri, if any.
        """
        if self.uris is None:
            return None

        local_uris = [uri for uri in self.uris if uri.startswith('file:')]
        if local_uris:
            return local_uris[0]

        return None

    @property
    def local_path(self) -> Optional[Path]:
        """
        A path to this dataset on the local filesystem (if available).
        """
        return uri_to_local_path(self.local_uri)

    @property
    def id(self) -> UUID:
        """ UUID of a dataset
        """
        # This is a string in a raw document.
        return UUID(self.metadata.id)

    @property
    def managed(self) -> bool:
        return self.type.managed

    @property
    def format(self) -> str:
        return self.metadata.format

    @property
    def uri_scheme(self) -> str:
        if self.uris is None or len(self.uris) == 0:
            return ''

        url = urlparse(self.uris[0])
        if url.scheme == '':
            return 'file'
        return url.scheme

    @property
    def measurements(self) -> Dict[str, Any]:
        # It's an optional field in documents.
        # Dictionary of key -> measurement descriptor
        if not hasattr(self.metadata, 'measurements'):
            return {}
        return self.metadata.measurements

    @cached_property
    def center_time(self) -> Optional[datetime]:
        """ mid-point of time range
        """
        time = self.time
        if time is None:
            return None
        return time.begin + (time.end - time.begin) // 2

    @property
    def time(self) -> Optional[Range]:
        try:
            time = self.metadata.time
            return Range(parse_time(time.begin), parse_time(time.end))
        except AttributeError:
            return None

    @cached_property
    def key_time(self):
        """
        :rtype: datetime.datetime
        """
        if 'key_time' in self.metadata.fields:
            return self.metadata.key_time

        # Existing datasets are already using the computed "center_time" for their storage index key
        # if 'center_time' in self.metadata.fields:
        #     return self.metadata.center_time

        return self.center_time

    @property
    def bounds(self) -> Optional[geometry.BoundingBox]:
        """ :returns: bounding box of the dataset in the native crs
        """
        gs = self._gs
        if gs is None:
            return None

        bounds = gs['geo_ref_points']
        return geometry.BoundingBox(left=min(bounds['ur']['x'], bounds['ll']['x']),
                                    right=max(bounds['ur']['x'], bounds['ll']['x']),
                                    top=max(bounds['ur']['y'], bounds['ll']['y']),
                                    bottom=min(bounds['ur']['y'], bounds['ll']['y']))

    @property
    def transform(self) -> Optional[Affine]:
        geo = self._gs
        if geo is None:
            return None

        bounds = geo.get('geo_ref_points')
        if bounds is None:
            return None

        return Affine(bounds['lr']['x'] - bounds['ul']['x'], 0, bounds['ul']['x'],
                      0, bounds['lr']['y'] - bounds['ul']['y'], bounds['ul']['y'])

    @property
    def is_archived(self) -> bool:
        """
        Is this dataset archived?

        (an archived dataset is one that is not intended to be used by users anymore: eg. it has been
        replaced by another dataset. It will not show up in search results, but still exists in the
        system via provenance chains or through id lookup.)

        """
        return self.archived_time is not None

    @property
    def is_active(self) -> bool:
        """
        Is this dataset active?

        (ie. dataset hasn't been archived)

        """
        return not self.is_archived

    @property
    def _gs(self) -> Optional[Dict[str, Any]]:
        try:
            return self.metadata.grid_spatial
        except AttributeError:
            return None

    @property
    def crs(self) -> Optional[geometry.CRS]:
        """ Return CRS if available
        """
        projection = self._gs

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
    def extent(self) -> Optional[geometry.Geometry]:
        """ :returns: valid extent of the dataset or None
        """

        def xytuple(obj):
            return obj['x'], obj['y']

        # If no projection or crs, they have no extent.
        projection = self._gs
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

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        str_loc = 'not available' if not self.uris else self.uris[0]
        return "Dataset <id={id} type={type} location={loc}>".format(id=self.id,
                                                                     type=self.type.name,
                                                                     loc=str_loc)

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def metadata(self) -> DocReader:
        if self.metadata_type is None:
            raise ValueError('Can not interpret dataset without metadata type set')
        return self.metadata_type.dataset_reader(self.metadata_doc)

    def metadata_doc_without_lineage(self) -> Dict[str, Any]:
        """ Return metadata document without nested lineage datasets
        """
        return without_lineage_sources(self.metadata_doc, self.metadata_type)


class Measurement(dict):
    """
    Describes a single data variable of a Product or Dataset.

    Must include, which can be used when loading and interpreting data:

     - name
     - dtype - eg: int8, int16, float32
     - nodata - What value represent No Data
     - units

    Attributes can be accessed using ``dict []`` syntax.

    Can also include attributes like alternative names 'aliases', and spectral and bit flags
    definitions to aid with interpreting the data.

    """
    REQUIRED_KEYS = ('name', 'dtype', 'nodata', 'units')
    OPTIONAL_KEYS = ('aliases', 'spectral_definition', 'flags_definition')
    ATTR_BLACKLIST = set(['name', 'dtype', 'aliases', 'resampling_method', 'fuser'])

    def __init__(self, **kwargs):
        missing_keys = set(self.REQUIRED_KEYS) - set(kwargs)
        if missing_keys:
            raise ValueError("Measurement required keys missing: {}".format(missing_keys))

        super().__init__(**kwargs)

    def __getattr__(self, key: str) -> Any:
        """ Allow access to items as attributes. """
        v = self.get(key, self)
        if v is self:
            raise AttributeError("'Measurement' object has no attribute '{}'".format(key))
        return v

    def __repr__(self) -> str:
        return "Measurement({})".format(super(Measurement, self).__repr__())

    def copy(self) -> 'Measurement':
        """Required as the super class `dict` method returns a `dict`
           and does not preserve Measurement class"""
        return Measurement(**self)

    def dataarray_attrs(self) -> Dict[str, Any]:
        """This returns attributes filtered for display in a dataarray."""
        return {key: value for key, value in self.items() if key not in self.ATTR_BLACKLIST}


@schema_validated(SCHEMA_PATH / 'metadata-type-schema.yaml')
class MetadataType(object):
    """Metadata Type definition"""

    def __init__(self,
                 definition: Mapping[str, Any],
                 dataset_search_fields: Mapping[str, Field],
                 id_: Optional[int] = None):
        self.definition = definition
        self.dataset_fields = dataset_search_fields
        self.id = id_

    @property
    def name(self) -> str:
        return self.definition['name']

    @property
    def description(self) -> str:
        return self.definition['description']

    def dataset_reader(self, dataset_doc: Mapping[str, Field]) -> DocReader:
        return DocReader(self.definition['dataset'], self.dataset_fields, dataset_doc)

    def __str__(self) -> str:
        return "MetadataType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self) -> str:
        return str(self)


@schema_validated(SCHEMA_PATH / 'dataset-type-schema.yaml')
class DatasetType(object):
    """
    Product definition

    :param MetadataType metadata_type:
    :param dict definition:
    """

    def __init__(self,
                 metadata_type: MetadataType,
                 definition: Mapping[str, Any],
                 id_: Optional[int] = None):
        assert isinstance(metadata_type, MetadataType)
        self.id = id_
        self.metadata_type = metadata_type
        #: product definition document
        self.definition = definition

    @property
    def name(self) -> str:
        return self.definition['name']

    @property
    def managed(self) -> bool:
        return self.definition.get('managed', False)

    @property
    def metadata_doc(self) -> Mapping[str, Any]:
        return self.definition['metadata']

    @property
    def metadata(self) -> DocReader:
        return self.metadata_type.dataset_reader(self.metadata_doc)

    @property
    def fields(self):
        return self.metadata_type.dataset_reader(self.metadata_doc).fields

    @property
    def measurements(self) -> Mapping[str, Measurement]:
        """
        Dictionary of measurements in this product
        """
        return OrderedDict((m['name'], Measurement(**m)) for m in self.definition.get('measurements', []))

    @property
    def dimensions(self) -> Tuple[str, str]:
        """
        List of dimension labels for data in this product
        """
        assert self.metadata_type.name == 'eo'
        if self.grid_spec is not None:
            spatial_dims = self.grid_spec.dimensions
        else:
            spatial_dims = DEFAULT_SPATIAL_DIMS

        return ('time',) + spatial_dims

    @cached_property
    def grid_spec(self) -> Optional['GridSpec']:
        """
        Grid specification for this product
        """
        storage = self.definition.get('storage')
        if storage is None:
            return None

        crs = storage.get('crs')
        if crs is None:
            return None

        crs = geometry.CRS(str(crs).strip())

        def extract_point(name):
            xx = storage.get(name, None)
            return None if xx is None else tuple(xx[dim] for dim in crs.dimensions)

        gs_params = {name: extract_point(name)
                     for name in ('tile_size', 'resolution', 'origin')}

        return GridSpec(crs=crs, **gs_params)

    def canonical_measurement(self, measurement: str) -> str:
        """ resolve measurement alias into canonical name
        """
        for m in self.measurements:
            if measurement == m:
                return measurement
            elif measurement in self.measurements[m].get('aliases', []):
                return m
        raise KeyError(measurement)

    def lookup_measurements(self, measurements: Optional[List[str]] = None) -> Mapping[str, Measurement]:
        """
        Find measurements by name

        :param measurements: list of measurement names
        """
        my_measurements = self.measurements
        if measurements is None:
            return my_measurements
        canonical = [self.canonical_measurement(measurement) for measurement in measurements]
        return OrderedDict((measurement, my_measurements[measurement]) for measurement in canonical)

    def dataset_reader(self, dataset_doc):
        return self.metadata_type.dataset_reader(dataset_doc)

    def to_dict(self) -> Mapping[str, Any]:
        """
        Convert to a dictionary representation of the available fields
        """
        row = {
            'id': self.id,
            'name': self.name,
            'description': self.definition['description'],
        }
        row.update(self.fields)
        if self.grid_spec is not None:
            row.update({
                'crs': str(self.grid_spec.crs),
                'spatial_dimensions': self.grid_spec.dimensions,
                'tile_size': self.grid_spec.tile_size,
                'resolution': self.grid_spec.resolution,
            })
        return row

    def __str__(self) -> str:
        return "DatasetType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self) -> str:
        return self.__str__()

    # Types are uniquely identifiable by name:

    def __eq__(self, other) -> bool:
        if self is other:
            return True

        if self.__class__ != other.__class__:
            return False

        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


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

    def __init__(self,
                 crs: geometry.CRS,
                 tile_size: Tuple[float, float],
                 resolution: Tuple[float, float],
                 origin: Optional[Tuple[float, float]] = None):
        self.crs = crs
        self.tile_size = tile_size
        self.resolution = resolution
        self.origin = origin or (0.0, 0.0)

    def __eq__(self, other):
        if not isinstance(other, GridSpec):
            return False

        return (self.crs == other.crs
                and self.tile_size == other.tile_size
                and self.resolution == other.resolution
                and self.origin == other.origin)

    @property
    def dimensions(self) -> Tuple[str, str]:
        """
        List of dimension names of the grid spec

        """
        return self.crs.dimensions

    @property
    def alignment(self) -> Tuple[float, float]:
        """
        Pixel boundary alignment
        """
        y, x = (orig % abs(res) for orig, res in zip(self.origin, self.resolution))
        return (y, x)

    @property
    def tile_resolution(self) -> Tuple[float, float]:
        """
        Tile size in pixels in CRS dimension order (Usually y,x or lat,lon)
        """
        y, x = (int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution))
        return (y, x)

    def tile_coords(self, tile_index: Tuple[int, int]) -> Tuple[float, float]:
        """
        Tile coordinates in (Y,X) order

        :param tile_index: in X,Y order
        """

        def coord(index: int,
                  resolution: float,
                  size: float,
                  origin: float) -> float:
            return (index + (1 if resolution < 0 < size else 0)) * size + origin

        y, x = (coord(index, res, size, origin)
                for index, res, size, origin in zip(tile_index[::-1], self.resolution, self.tile_size, self.origin))
        return (y, x)

    def tile_geobox(self, tile_index: Tuple[int, int]) -> geometry.GeoBox:
        """
        Tile geobox.

        :param (int,int) tile_index:
        """
        res_y, res_x = self.resolution
        y, x = self.tile_coords(tile_index)
        h, w = self.tile_resolution
        geobox = geometry.GeoBox(crs=self.crs, affine=Affine(res_x, 0.0, x, 0.0, res_y, y), width=w, height=h)
        return geobox

    def tiles(self, bounds: geometry.BoundingBox,
              geobox_cache: Optional[dict] = None) -> Iterator[Tuple[Tuple[int, int],
                                                                     geometry.GeoBox]]:
        """
        Returns an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid and overlapping with the specified `bounds` rectangle.

        .. note::

           Grid cells are referenced by coordinates `(x, y)`, which is the opposite to the usual CRS
           dimension order.

        :param BoundingBox bounds: Boundary coordinates of the required grid
        :param dict geobox_cache: Optional cache to re-use geoboxes instead of creating new one each time
        :return: iterator of grid cells with :py:class:`GeoBox` tiles
        """
        def geobox(tile_index):
            if geobox_cache is None:
                return self.tile_geobox(tile_index)

            gbox = geobox_cache.get(tile_index)
            if gbox is None:
                gbox = self.tile_geobox(tile_index)
                geobox_cache[tile_index] = gbox
            return gbox

        tile_size_y, tile_size_x = self.tile_size
        tile_origin_y, tile_origin_x = self.origin
        for y in GridSpec.grid_range(bounds.bottom - tile_origin_y, bounds.top - tile_origin_y, tile_size_y):
            for x in GridSpec.grid_range(bounds.left - tile_origin_x, bounds.right - tile_origin_x, tile_size_x):
                tile_index = (x, y)
                yield tile_index, geobox(tile_index)

    def tiles_from_geopolygon(self, geopolygon: geometry.Geometry,
                              tile_buffer: Optional[Tuple[float, float]] = None,
                              geobox_cache: Optional[dict] = None) -> Iterator[Tuple[Tuple[int, int],
                                                                                     geometry.GeoBox]]:
        """
        Returns an iterator of tile_index, :py:class:`GeoBox` tuples across
        the grid and overlapping with the specified `geopolygon`.

        .. note::

           Grid cells are referenced by coordinates `(x, y)`, which is the opposite to the usual CRS
           dimension order.

        :param geometry.Geometry geopolygon: Polygon to tile
        :param tile_buffer: Optional <float,float> tuple, (extra padding for the query
                            in native units of this GridSpec)
        :param dict geobox_cache: Optional cache to re-use geoboxes instead of creating new one each time
        :return: iterator of grid cells with :py:class:`GeoBox` tiles
        """
        geopolygon = geopolygon.to_crs(self.crs)
        bbox = geopolygon.boundingbox
        bbox = bbox.buffered(*tile_buffer) if tile_buffer else bbox

        for tile_index, tile_geobox in self.tiles(bbox, geobox_cache):
            tile_geobox = tile_geobox.buffered(*tile_buffer) if tile_buffer else tile_geobox

            if geometry.intersects(tile_geobox.extent, geopolygon):
                yield (tile_index, tile_geobox)

    @staticmethod
    def grid_range(lower: float, upper: float, step: float) -> range:
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

    def __str__(self) -> str:
        return "GridSpec(crs=%s, tile_size=%s, resolution=%s)" % (
            self.crs, self.tile_size, self.resolution)

    def __repr__(self) -> str:
        return self.__str__()


def metadata_from_doc(doc: Mapping[str, Any]) -> MetadataType:
    """Construct MetadataType that is not tied to any particular db index. This is
    useful when there is a need to interpret dataset metadata documents
    according to metadata spec.
    """
    from .fields import get_dataset_fields
    MetadataType.validate(doc)  # type: ignore
    return MetadataType(doc, get_dataset_fields(doc))
