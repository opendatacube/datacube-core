# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core classes used across modules.
"""

import logging
import math
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from uuid import UUID

from affine import Affine
from typing import Optional, List, Mapping, Any, Dict, Tuple, Iterator, Iterable, Union

from urllib.parse import urlparse
from datacube.utils import geometry, without_lineage_sources, parse_time, cached_property, uri_to_local_path, \
    schema_validated, DocReader
from .fields import Field, get_dataset_fields
from ._base import Range, ranges_overlap  # noqa: F401

_LOG = logging.getLogger(__name__)

DEFAULT_SPATIAL_DIMS = ('y', 'x')  # Used when product lacks grid_spec

SCHEMA_PATH = Path(__file__).parent / 'schema'


# TODO: Multi-dimension code is has incomplete type hints and significant type issues that will require attention

class Dataset:
    """
    A Dataset. A container of metadata, and refers typically to a multi-band raster on disk.

    Most important parts are the metadata_doc and uri.

    :param metadata_doc: the document (typically a parsed json/yaml)
    :param uris: All active uris for the dataset
    """

    def __init__(self,
                 type_: 'Product',
                 metadata_doc: Dict[str, Any],
                 uris: Optional[List[str]] = None,
                 sources: Optional[Mapping[str, 'Dataset']] = None,
                 indexed_by: Optional[str] = None,
                 indexed_time: Optional[datetime] = None,
                 archived_time: Optional[datetime] = None):
        assert isinstance(type_, Product)

        self.type = type_

        #: The document describing the dataset as a dictionary. It is often serialised as YAML on disk
        #: or inside a NetCDF file, and as JSON-B inside the database index.
        self.metadata_doc = metadata_doc

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
    def metadata_type(self) -> 'MetadataType':
        return self.type.metadata_type

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
        metadata = self.metadata
        if not hasattr(metadata, 'measurements'):
            return {}
        return metadata.measurements

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
        if isinstance(other, Dataset):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        str_loc = 'not available' if not self.uris else self.uris[0]
        return "Dataset <id={id} product={type} location={loc}>".format(id=self.id,
                                                                        type=self.type.name,
                                                                        loc=str_loc)

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def metadata(self) -> DocReader:
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
    OPTIONAL_KEYS = ('aliases', 'spectral_definition', 'flags_definition', 'scale_factor', 'add_offset',
                     'extra_dim')
    ATTR_SKIP = set(['name', 'dtype', 'aliases', 'resampling_method', 'fuser', 'extra_dim', 'extra_dim_index'])

    def __init__(self, canonical_name=None, **kwargs):
        missing_keys = set(self.REQUIRED_KEYS) - set(kwargs)
        if missing_keys:
            raise ValueError("Measurement required keys missing: {}".format(missing_keys))

        self.canonical_name = canonical_name or kwargs.get('name')
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
        return {key: value for key, value in self.items() if key not in self.ATTR_SKIP}


@schema_validated(SCHEMA_PATH / 'metadata-type-schema.yaml')
class MetadataType:
    """Metadata Type definition"""

    def __init__(self,
                 definition: Mapping[str, Any],
                 dataset_search_fields: Optional[Mapping[str, Field]] = None,
                 id_: Optional[int] = None):
        if dataset_search_fields is None:
            dataset_search_fields = get_dataset_fields(definition)
        self.definition = definition
        self.dataset_fields = dataset_search_fields
        self.id = id_

    @property
    def name(self) -> str:
        return self.definition.get('name', None)

    @property
    def description(self) -> str:
        return self.definition.get('description', None)

    def dataset_reader(self, dataset_doc: Mapping[str, Field]) -> DocReader:
        return DocReader(self.definition['dataset'], self.dataset_fields, dataset_doc)

    def __str__(self) -> str:
        return "MetadataType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self) -> str:
        return str(self)


@schema_validated(SCHEMA_PATH / 'dataset-type-schema.yaml')
class Product:
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
        self._extra_dimensions: Optional[Mapping[str, Any]] = None
        self._canonical_measurements: Optional[Mapping[str, Measurement]] = None
        self._all_measurements: Optional[Dict[str, Measurement]] = None
        self._load_hints: Optional[Dict[str, Any]] = None

    def _resolve_aliases(self):
        if self._all_measurements is not None:
            return self._all_measurements
        mm = self.measurements
        oo = {}

        for m in mm.values():
            oo[m.name] = m
            for alias in m.get('aliases', []):
                # TODO: check for duplicates
                # if alias is in oo already -- bad
                m_alias = dict(**m)
                m_alias.update(name=alias, canonical_name=m.name)
                oo[alias] = Measurement(**m_alias)

        self._all_measurements = oo
        return self._all_measurements

    @property
    def name(self) -> str:
        return self.definition['name']

    @property
    def description(self) -> str:
        return self.definition.get("description", None)

    @property
    def license(self) -> str:
        return self.definition.get("license", None)

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
        # from copy import deepcopy
        if self._canonical_measurements is None:
            def fix_nodata(m):
                nodata = m.get('nodata', None)
                if isinstance(nodata, str):
                    m = dict(**m)
                    m['nodata'] = float(nodata)
                return m

            self._canonical_measurements = OrderedDict((m['name'], Measurement(**fix_nodata(m)))
                                                       for m in self.definition.get('measurements', []))

        return self._canonical_measurements

    @property
    def dimensions(self) -> Tuple[str, str, str]:
        """
        List of dimension labels for data in this product
        """
        if self.grid_spec is not None:
            spatial_dims = self.grid_spec.dimensions
        else:
            spatial_dims = DEFAULT_SPATIAL_DIMS

        return ('time',) + spatial_dims

    @property
    def extra_dimensions(self) -> "ExtraDimensions":
        """
        Dictionary of metadata for the third dimension.
        """
        if self._extra_dimensions is None:
            self._extra_dimensions = OrderedDict((d['name'], d)
                                                 for d in self.definition.get('extra_dimensions', []))
        return ExtraDimensions(self._extra_dimensions)

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

        complete = all(gs_params[k] is not None for k in ('tile_size', 'resolution'))
        if not complete:
            return None

        return GridSpec(crs=crs, **gs_params)

    @staticmethod
    def validate_extra_dims(definition: Mapping[str, Any]):
        """Validate 3D metadata in the product definition.

        Perform some basic checks for validity of the 3D dataset product definition:
          - Checks extra_dimensions section exists
          - For each 3D measurement, check if the required dimension is defined
          - If the 3D spectral_definition is defined:
            - Check there's one entry per coordinate.
            - Check that wavelength and response are the same length.

        :param definition: Dimension definition dict, typically retrieved from the product definition's
            `extra_dimensions` field.
        """
        # Dict of extra dimensions names and values in the product definition
        defined_extra_dimensions = OrderedDict(
            (d.get("name"), d.get("values")) for d in definition.get("extra_dimensions", [])
        )

        for m in definition.get('measurements', []):
            # Skip if not a 3D measurement
            if 'extra_dim' not in m:
                continue

            # Found 3D measurement, check if extra_dimension is defined.
            if (len(defined_extra_dimensions) == 0):
                raise ValueError(
                    "extra_dimensions is not defined. 3D measurements require extra_dimensions "
                    "to be defined for the dimension"
                )

            dim_name = m.get('extra_dim')

            # Check extra dimension is defined
            if dim_name not in defined_extra_dimensions:
                raise ValueError(f"Dimension {dim_name} is not defined in extra_dimensions")

            if 'spectral_definition' in m:
                spectral_definitions = m.get('spectral_definition', [])
                # Check spectral_definition of expected length
                if len(defined_extra_dimensions[dim_name]) != len(spectral_definitions):
                    raise ValueError(
                        f"spectral_definition should be the same length as values for extra_dim {m.get('extra_dim')}"
                    )

                # Check each spectral_definition has the same length for wavelength and response if both exists
                for idx, spectral_definition in enumerate(spectral_definitions):
                    if 'wavelength' in spectral_definition and 'response' in spectral_definition:
                        if len(spectral_definition.get('wavelength')) != len(spectral_definition.get('response')):
                            raise ValueError(
                                f"spectral_definition_map: wavelength should be the same length as response "
                                f"in the product definition for spectral definition at index {idx}."
                            )

    def canonical_measurement(self, measurement: str) -> str:
        """ resolve measurement alias into canonical name
        """
        m = self._resolve_aliases().get(measurement, None)
        if m is None:
            raise ValueError(f"No such band/alias {measurement}")

        return m.canonical_name

    def lookup_measurements(
        self, measurements: Optional[Union[Iterable[str], str]] = None
    ) -> Mapping[str, Measurement]:
        """
        Find measurements by name

        :param measurements: list of measurement names or a single measurement name, or None to get all
        """
        if measurements is None:
            return self.measurements
        if isinstance(measurements, str):
            measurements = [measurements]

        mm = self._resolve_aliases()
        return OrderedDict((m, mm[m]) for m in measurements)

    def _extract_load_hints(self) -> Optional[Dict[str, Any]]:
        _load = self.definition.get('load')
        if _load is None:
            # Check for partial "storage" definition
            storage = self.definition.get('storage', {})

            if 'crs' in storage and 'resolution' in storage:
                if 'tile_size' in storage:
                    # Fully defined GridSpec, ignore it
                    return None

                # TODO: warn user to use `load:` instead of `storage:`??
                _load = storage
            else:
                return None

        crs = geometry.CRS(_load['crs'])

        def extract_point(name):
            xx = _load.get(name, None)
            return None if xx is None else tuple(xx[dim] for dim in crs.dimensions)

        params = {name: extract_point(name) for name in ('resolution', 'align')}
        params = {name: v for name, v in params.items() if v is not None}
        return dict(crs=crs, **params)

    @property
    def default_crs(self) -> Optional[geometry.CRS]:
        return self.load_hints().get('output_crs', None)

    @property
    def default_resolution(self) -> Optional[Tuple[float, float]]:
        return self.load_hints().get('resolution', None)

    @property
    def default_align(self) -> Optional[Tuple[float, float]]:
        return self.load_hints().get('align', None)

    def load_hints(self) -> Dict[str, Any]:
        """
        Returns dictionary with keys compatible with ``dc.load(..)`` named arguments:

          output_crs - CRS
          resolution - Tuple[float, float]
          align      - Tuple[float, float] (if defined)

        Returns {} if load hints are not defined on this product, or defined with errors.
        """
        if self._load_hints is not None:
            return self._load_hints

        hints = None
        try:
            hints = self._extract_load_hints()
        except Exception:
            pass

        if hints is None:
            self._load_hints = {}
        else:
            crs = hints.pop('crs')
            self._load_hints = dict(output_crs=crs, **hints)

        return self._load_hints

    def dataset_reader(self, dataset_doc):
        return self.metadata_type.dataset_reader(dataset_doc)

    def to_dict(self) -> Mapping[str, Any]:
        """
        Convert to a dictionary representation of the available fields
        """
        row = dict(**self.fields)
        row.update(id=self.id,
                   name=self.name,
                   license=self.license,
                   description=self.description)

        if self.grid_spec is not None:
            row.update({
                'crs': str(self.grid_spec.crs),
                'spatial_dimensions': self.grid_spec.dimensions,
                'tile_size': self.grid_spec.tile_size,
                'resolution': self.grid_spec.resolution,
            })
        return row

    def __str__(self) -> str:
        return "Product(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

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


# Type alias for backwards compatibility
DatasetType = Product


@schema_validated(SCHEMA_PATH / 'ingestor-config-type-schema.yaml')
class IngestorConfig:
    """
    Ingestor configuration definition
    """
    pass


class GridSpec:
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
    def tile_resolution(self) -> Tuple[int, int]:
        """
        Tile size in pixels in CRS dimension order (Usually y,x or lat,lon)
        """
        y, x = (int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution))
        return (y, x)

    def tile_coords(self, tile_index: Tuple[int, int]) -> Tuple[float, float]:
        """
        Coordinate of the top-left corner of the tile in (Y,X) order

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


class ExtraDimensions:
    """
    Definition for the additional dimensions between (t) and (y, x)

    It allows the creation of a subsetted ExtraDimensions that contains slicing information relative to
    the original dimension coordinates.
    """

    def __init__(self, extra_dim: Mapping[str, Any]):
        """Init function

        :param extra_dim: Dimension definition dict, typically retrieved from the product definition's
            `extra_dimensions` field.
        """
        import xarray

        # Dict of information about each dimension
        self._dims = extra_dim
        # Dimension slices that results in this ExtraDimensions object
        self._dim_slice = {
            name: (0, len(dim['values'])) for name, dim in extra_dim.items()
        }
        # Coordinate information
        self._coords = {
            name: xarray.DataArray(
                data=dim['values'],
                coords={name: dim['values']},
                dims=(name,),
                name=name,
            ).astype(dim['dtype'])
            for name, dim in extra_dim.items()
        }

    def has_empty_dim(self) -> bool:
        """Return True if ExtraDimensions has an empty dimension, otherwise False.

        :return: A boolean if ExtraDimensions has an empty dimension, otherwise False.
        """
        for value in self._coords.values():
            if value.shape[0] == 0:
                return True
        return False

    def __getitem__(self, dim_slices: Dict[str, Union[float, Tuple[float, float]]]) -> "ExtraDimensions":
        """Return a ExtraDimensions subsetted by dim_slices

        :param dim_slices: Dict of dimension slices to subset by.
        :return: An ExtraDimensions object subsetted by `dim_slices`
        """
        # Check all dimensions specified in dim_slices exists
        unknown_keys = set(dim_slices.keys()) - set(self._dims.keys())
        if unknown_keys:
            raise KeyError(f"Found unknown keys {unknown_keys} in dim_slices")

        from copy import deepcopy

        ed = ExtraDimensions(deepcopy(self._dims))
        ed._dim_slice = self._dim_slice

        # Convert to integer index
        for dim_name, dim_slice in dim_slices.items():
            dim_slices[dim_name] = self.coord_slice(dim_name, dim_slice)

        for dim_name, dim_slice in dim_slices.items():
            # Adjust slices relative to original.
            if dim_name in ed._dim_slice:
                ed._dim_slice[dim_name] = (    # type: ignore[assignment]
                    ed._dim_slice[dim_name][0] + dim_slice[0],  # type: ignore[index]
                    ed._dim_slice[dim_name][0] + dim_slice[1],  # type: ignore[index]
                )

            # Subset dimension values.
            if dim_name in ed._dims:
                ed._dims[dim_name]['values'] = ed._dims[dim_name]['values'][slice(*dim_slice)]  # type: ignore[misc]

            # Subset dimension coordinates.
            if dim_name in ed._coords:
                slice_dict = {k: slice(*v) for k, v in dim_slices.items()}  # type: ignore[misc]
                ed._coords[dim_name] = ed._coords[dim_name].isel(slice_dict)

        return ed

    @property
    def dims(self) -> Mapping[str, dict]:
        """Returns stored dimension information

        :return: A dict of information about each dimension
        """
        return self._dims

    @property
    def dim_slice(self) -> Mapping[str, Tuple[int, int]]:
        """Returns dimension slice for this ExtraDimensions object

        :return: A dict of dimension slices that results in this ExtraDimensions object
        """
        return self._dim_slice

    def measurements_values(self, dim: str) -> List[Any]:
        """Returns the dimension values after slicing

        :param dim: The name of the dimension
        :return: A list of dimension values for the requested dimension.
        """
        if dim not in self._dims:
            raise ValueError(f"Dimension {dim} not found.")
        return self._dims[dim]['values']

    def measurements_slice(self, dim: str) -> slice:
        """Returns the index for slicing on a dimension

        :param dim: The name of the dimension
        :return: A slice for the the requested dimension.
        """
        dim_slice = self.measurements_index(dim)
        return slice(*dim_slice)

    def measurements_index(self, dim: str) -> Tuple[int, int]:
        """Returns the index for slicing on a dimension as a tuple.

        :param dim: The name of the dimension
        :return: A tuple for the the requested dimension.
        """
        if dim not in self._dim_slice:
            raise ValueError(f"Dimension {dim} not found.")

        dim_slice = self._dim_slice[dim]
        return dim_slice

    def index_of(self, dim: str, value: Any) -> int:
        """Find index for value in the dimension dim

        :param dim: The name of the dimension
        :param value: The coordinate value.
        :return: The integer index of `value`
        """
        if dim not in self._coords:
            raise ValueError(f"Dimension {dim} not found.")
        return self._coords[dim].searchsorted(value)

    def coord_slice(self, dim: str, coord_range: Union[float, Tuple[float, float]]) -> Tuple[int, int]:
        """Returns the Integer index for a coordinate (min, max) range.

        :param dim: The name of the dimension
        :param coord_range: The coordinate range.
        :return: A tuple containing the integer indexes of `coord_range.
        """
        # Convert to Tuple if it's an int or float
        if isinstance(coord_range, int) or isinstance(coord_range, float):
            coord_range = (coord_range, coord_range)

        start_index = self.index_of(dim, coord_range[0])
        stop_index = self.index_of(dim, coord_range[1] + 1)
        return start_index, stop_index

    def chunk_size(self) -> Tuple[Tuple[str, ...], Tuple[int, ...]]:
        """Returns the names and shapes of dimenions in dimension order

        :return: A tuple containing the names and max sizes of each dimension
        """
        names = ()
        shapes = ()
        if self.dims is not None:
            for dim in self.dims.values():
                name = dim.get('name')
                names += (name,)   # type: ignore[assignment]
                shapes += (len(self.measurements_values(name)),)   # type: ignore[assignment,arg-type]
        return names, shapes

    def __str__(self) -> str:
        return (
            f"ExtraDimensions(extra_dim={dict(self._dims)}, dim_slice={self._dim_slice} "
            f"coords={self._coords} "
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()
