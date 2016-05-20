# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import, division

import math
import codecs
import logging
import os
from collections import namedtuple, defaultdict

import dateutil.parser
import numpy
from affine import Affine
from osgeo import osr
from pathlib import Path
from rasterio.coords import BoundingBox

from datacube import compat
from datacube.compat import parse_url
from datacube.utils import datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('dtype', 'begin', 'end', 'length', 'units'))
CoordinateValue = namedtuple('CoordinateValue', ('dimension_name', 'value', 'dtype', 'units'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dimensions', 'units'))

NETCDF_VAR_OPTIONS = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous'}
VALID_VARIABLE_ATTRS = {'standard_name', 'long_name', 'units', 'flags_definition'}


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


class DatasetMatcher(object):
    def __init__(self, metadata):
        # Match by exact metadata properties (a subset of the metadata doc)
        #: :type: dict
        self.metadata = metadata

    def __repr__(self):
        return "{}(metadata={!r})".format(self.__class__.__name__, self.metadata)


class Dataset(object):
    def __init__(self, type_, metadata_doc, local_uri, sources=None, managed=False):
        """
        A dataset on disk.

        :type type_: DatasetType
        :param metadata_doc: the document (typically a parsed json/yaml)
        :type metadata_doc: dict
        :param local_uri: A URI to access this dataset locally.
        :param managed: Should dataset files be managed by the datacube instance (vs managed externally)
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

        #: :type: bool
        self.managed = managed

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
        return self.metadata_doc['id']

    @property
    def format(self):
        return self.metadata_doc['format']['name']

    @property
    def measurements(self):
        return self.metadata.measurements_dict

    @property
    def time(self):
        center_dt = self.metadata_doc['extent']['center_dt']
        if isinstance(center_dt, compat.string_types):
            center_dt = dateutil.parser.parse(center_dt)
        return center_dt

    @property
    def bounds(self):
        geo_ref_points = self.metadata_doc['grid_spatial']['projection']['geo_ref_points']
        return BoundingBox(geo_ref_points['ll']['x'], geo_ref_points['ll']['y'],
                           geo_ref_points['ur']['x'], geo_ref_points['ur']['y'])

    @property
    def crs(self):
        """
        "rtype: datacube.model.CRS
        :return:
        """
        projection = self.metadata_doc['grid_spatial']['projection']

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

        geo_ref_points = self.metadata_doc['grid_spatial']['projection']['geo_ref_points']
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
        return _DocReader(self.dataset_offsets.__dict__, dataset_doc)

    def __str__(self):
        return "MetadataType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)


class DatasetType(object):
    def __init__(self,
                 metadata_type,
                 definition,
                 id_=None):
        """
        DatasetType of datasets & storage.

        :type metadata_type: MetadataType
        """
        self.id = id_

        # All datasets in a collection must have the same metadata_type.
        self.metadata_type = metadata_type

        # DatasetType definition.
        self.definition = definition

    @property
    def name(self):
        return self.definition['name']

    @property
    def metadata(self):
        return self.definition['metadata']

    @property
    def measurements(self):
        return self.definition.get('measurements', {})

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

    def __str__(self):
        return "DatasetType(name={name!r}, id_={id!r})".format(id=self.id, name=self.name)

    def __repr__(self):
        return self.__str__()


class DatasetOffsets(object):
    """
    Where to find certain mandatory fields in dataset metadata.
    """

    def __init__(self,
                 uuid_field=None,
                 label_field=None,
                 creation_time_field=None,
                 measurements_dict=None,
                 sources=None):
        # UUID for a dataset. Always unique.
        #: :type: tuple[string]
        self.uuid_field = uuid_field or ('id',)

        # The dataset "label" is the logical identifier for a dataset.
        #
        # -> Multiple datasets may arrive with the same label, but only the 'latest' will be returned by default
        #    in searches.
        #
        # Use case: reprocessing a dataset.
        # -> When reprocessing a dataset, the new dataset should be produced with the same label as the old one.
        # -> Because you probably don't want both datasets returned from typical searches. (they are the same data)
        # -> When ingested, this reprocessed dataset will be the only one visible to typical searchers.
        # -> But the old dataset will still exist in the database for provenance & historical record.
        #       -> Existing higher-level/derived datasets will still link to the old dataset they were processed
        #          from, even if it's not the latest.
        #
        # An example label used by GA (called "dataset_ids" on historical systems):
        #      -> Eg. "LS7_ETM_SYS_P31_GALPGS01-002_114_73_20050107"
        #
        # But the collection owner can use any string to label their datasets.
        #: :type: tuple[string]
        self.label_field = label_field or ('label',)

        # datetime the dataset was processed/created.
        #: :type: tuple[string]
        self.creation_time_field = creation_time_field or ('creation_dt',)

        # Where to find a dict of measurements/bands in the dataset.
        #  -> Dict key is measurement/band id,
        #  -> Dict value is object with fields depending on the storage driver.
        #     (such as path to band file, offset within file etc.)
        #: :type: tuple[string]
        self.measurements_dict = measurements_dict or ('measurements',)

        # Where to find a dict of embedded source datasets
        #  -> The dict is of form: classifier->source_dataset_doc
        #  -> 'classifier' is how to classify/identify the relationship (usually the type of source it was eg. 'nbar').
        #      An arbitrary string, but you should be consistent between datasets (to query relationships).
        #: :type: tuple[string]
        self.sources = sources or ('lineage', 'source_datasets')


class GeoPolygon(object):
    """
    Polygon with a CRS
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
        :param crs_str:
        :return: new GeoPolygon with CRS specified by crs_str
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
    Wrapper around osr.SpatialReference providing more pythonic interface

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
        assert isinstance(other, self.__class__)
        return self._crs.IsSame(other._crs) == 1  # pylint: disable=protected-access


class GridSpec(object):
    def __init__(self, crs=None, tile_size=None, resolution=None):
        self.crs = crs
        self.tile_size = tile_size
        self.resolution = resolution

    @property
    def dimensions(self):
        return self.crs.dimensions

    @property
    def tile_resolution(self):
        return [int(abs(ts / res)) for ts, res in zip(self.tile_size, self.resolution)]

    def __str__(self):
        return "GridSpec(crs=%s, tile_size=%s, resolution=%s)" % (self.crs, self.tile_size, self.resolution)

    def __repr__(self):
        return self.__str__()


class GeoBox(object):
    """
    Defines a single Storage Unit, its CRS, location, resolution, and global attributes

    >>> from affine import Affine
    >>> t = GeoBox(4000, 4000, Affine(0.00025, 0.0, 151.0, 0.0, -0.00025, -29.0), CRS('EPSG:4326'))
    >>> t.coordinate_labels['latitude']
    array([-29.000125, -29.000375, -29.000625, ..., -29.999375, -29.999625,
           -29.999875])
    >>> t.coordinate_labels['longitude']
    array([ 151.000125,  151.000375,  151.000625, ...,  151.999375,
            151.999625,  151.999875])
    >>> t.geographic_extent.points
    [(151.0, -29.0), (151.0, -30.0), (152.0, -30.0), (152.0, -29.0)]


    :param crs_str: WKT representation of the coordinate reference system
    :type crs_str: str
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
    def from_geopolygon(cls, geopolygon, resolution, align=True):
        """
        :type geopolygon: datacube.model.GeoPolygon
        :param resolution: (x_resolution, y_resolution)
        :param align: Should the geobox be aligned to pixels of the given resolution. This assumes an origin of (0,0).
        :type align: boolean
        :rtype: GeoBox
        """
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
        return GeoBox(crs=geopolygon.crs,
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
        xs = numpy.array([0, self.width - 1]) * self.affine.a + self.affine.c + self.affine.a / 2
        ys = numpy.array([0, self.height - 1]) * self.affine.e + self.affine.f + self.affine.e / 2

        if self.crs.geographic:
            return {
                'latitude': Coordinate(ys.dtype, ys[0], ys[-1], self.height, 'degrees_north'),
                'longitude': Coordinate(xs.dtype, xs[0], xs[-1], self.width, 'degrees_east')
            }

        elif self.crs.projected:
            units = self.crs['UNIT']
            return {
                'x': Coordinate(xs.dtype, xs[0], xs[-1], self.width, units),
                'y': Coordinate(ys.dtype, ys[0], ys[-1], self.height, units)
            }

    @property
    def coordinate_labels(self):
        xs = numpy.arange(self.width) * self.affine.a + self.affine.c + self.affine.a / 2
        ys = numpy.arange(self.height) * self.affine.e + self.affine.f + self.affine.e / 2

        if self.crs.geographic:
            return {
                'latitude': ys,
                'longitude': xs
            }
        elif self.crs.projected:
            return {
                'x': xs,
                'y': ys
            }

    @property
    def geographic_extent(self):
        if self.crs.geographic:
            return self.extent
        return self.extent.to_crs(CRS('EPSG:4326'))


def _get_doc_offset(offset, document):
    """
    :type offset: list[str]
    :type document: dict

    >>> _get_doc_offset(['a'], {'a': 4})
    4
    >>> _get_doc_offset(['a', 'b'], {'a': {'b': 4}})
    4
    >>> _get_doc_offset(['a'], {})
    Traceback (most recent call last):
    ...
    KeyError: 'a'
    """
    value = document
    for key in offset:
        value = value[key]
    return value


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
    sub_doc = _get_doc_offset(read_offset, document)
    sub_doc[offset[-1]] = value


class _DocReader(object):
    def __init__(self, field_offsets, doc):
        """
        :type field_offsets: dict[str,list[str]]
        :type doc: dict
        >>> d = _DocReader({'lat': ['extent', 'lat']}, doc={'extent': {'lat': 4}})
        >>> d.lat
        4
        >>> d.lat = 5
        >>> d._doc
        {'extent': {'lat': 5}}
        >>> d.lon
        Traceback (most recent call last):
        ...
        ValueError: Unknown field 'lon'. Expected one of ['lat']
        """
        self.__dict__['_field_offsets'] = field_offsets
        self.__dict__['_doc'] = doc

    def __getattr__(self, name):
        offset = self._field_offsets.get(name)
        if offset is None:
            raise ValueError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._field_offsets.keys())
                )
            )
        return _get_doc_offset(offset, self._doc)

    def __setattr__(self, name, val):
        offset = self._field_offsets.get(name)
        if offset is None:
            raise ValueError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._field_offsets.keys())
                )
            )
        return _set_doc_offset(offset, self._doc, val)


def time_coordinate_value(time):
    return CoordinateValue(dimension_name='time',
                           value=datetime_to_seconds_since_1970(time),
                           dtype=numpy.dtype(numpy.float64),
                           units='seconds since 1970-01-01 00:00:00')
