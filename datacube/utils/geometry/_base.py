import functools
import itertools
import math
from collections import namedtuple, OrderedDict
from typing import Tuple, Iterable, List, Union, Optional, Any, Callable, Hashable, Dict
from collections.abc import Sequence
from distutils.version import LooseVersion

import cachetools
import numpy
import xarray as xr
from affine import Affine
import rasterio
from shapely import geometry, ops
from shapely.geometry import base
from pyproj import CRS as _CRS
from pyproj.enums import WktVersion
from pyproj.transformer import Transformer
from pyproj.exceptions import CRSError

from .tools import roi_normalise, roi_shape, is_affine_st
from ..math import is_almost_int

Coordinate = namedtuple('Coordinate', ('values', 'units', 'resolution'))
_BoundingBox = namedtuple('BoundingBox', ('left', 'bottom', 'right', 'top'))
SomeCRS = Union[str, 'CRS', _CRS]
MaybeCRS = Optional[SomeCRS]
CoordList = List[Tuple[float, float]]

# pylint: disable=too-many-lines


class BoundingBox(_BoundingBox):
    """Bounding box, defining extent in cartesian coordinates.
    """

    def buffered(self, ybuff, xbuff):
        """
        Return a new BoundingBox, buffered in the x and y dimensions.

        :param ybuff: Y dimension buffering amount
        :param xbuff: X dimension buffering amount
        :return: new BoundingBox
        """
        return BoundingBox(left=self.left - xbuff, right=self.right + xbuff,
                           top=self.top + ybuff, bottom=self.bottom - ybuff)

    @property
    def width(self):
        return self.right - self.left

    @property
    def height(self):
        return self.top - self.bottom

    @property
    def points(self) -> CoordList:
        """Extract four corners of the bounding box
        """
        x0, y0, x1, y1 = self
        return list(itertools.product((x0, x1), (y0, y1)))

    def transform(self, transform: Affine) -> 'BoundingBox':
        """Transform bounding box through a linear transform

           Apply linear transform on 4 points of the bounding box and compute
           bounding box of these four points.
        """
        pts = [transform*pt for pt in self.points]
        xx = [x for x, _ in pts]
        yy = [y for _, y in pts]
        return BoundingBox(min(xx), min(yy), max(xx), max(yy))


@cachetools.cached({})
def _make_crs(crs_str: str) -> Tuple[_CRS, Optional[int]]:
    crs = _CRS.from_user_input(crs_str)
    return (crs, crs.to_epsg())


def _make_crs_transform_key(from_crs, to_crs, always_xy):
    return (id(from_crs), id(to_crs), always_xy)


@cachetools.cached({}, key=_make_crs_transform_key)
def _make_crs_transform(from_crs, to_crs, always_xy):
    return Transformer.from_crs(from_crs, to_crs, always_xy=always_xy).transform


def _guess_crs_str(crs_spec: Any) -> Optional[str]:
    """
    Returns a string representation of the crs spec.
    Returns `None` if it does not understand the spec.
    """
    if isinstance(crs_spec, str):
        return crs_spec
    if hasattr(crs_spec, 'to_epsg'):
        epsg = crs_spec.to_epsg()
        if epsg is not None:
            return 'EPSG:{}'.format(crs_spec.to_epsg())
    if hasattr(crs_spec, 'to_wkt'):
        return crs_spec.to_wkt()
    return None


class CRS:
    """
    Wrapper around `pyproj.CRS` for backwards compatibility.
    """
    DEFAULT_WKT_VERSION = (WktVersion.WKT1_GDAL if LooseVersion(rasterio.__gdal_version__) < LooseVersion("3.0.0")
                           else WktVersion.WKT2_2019)

    __slots__ = ('_crs', '_epsg', 'crs_str')

    def __init__(self, crs_str: Any):
        """
        :param crs_str: string representation of a CRS, often an EPSG code like 'EPSG:4326'
        :raises: `pyproj.exceptions.CRSError`
        """
        crs_str = _guess_crs_str(crs_str)
        if crs_str is None:
            raise CRSError("Expect string or any object with `.to_epsg()` or `.to_wkt()` method")

        _crs, _epsg = _make_crs(crs_str)

        self._crs = _crs
        self._epsg = _epsg
        self.crs_str = crs_str

    def __getstate__(self):
        return {'crs_str': self.crs_str}

    def __setstate__(self, state):
        self.__init__(state['crs_str'])

    def to_wkt(self, pretty: bool = False, version: Optional[WktVersion] = None) -> str:
        """
        WKT representation of the CRS
        """
        if version is None:
            version = self.DEFAULT_WKT_VERSION

        return self._crs.to_wkt(pretty=pretty, version=version)

    @property
    def wkt(self) -> str:
        return self.to_wkt(version="WKT1_GDAL")

    def to_epsg(self) -> Optional[int]:
        """
        EPSG Code of the CRS or None
        """
        return self._epsg

    @property
    def epsg(self) -> Optional[int]:
        return self._epsg

    @property
    def semi_major_axis(self):
        return self._crs.ellipsoid.semi_major_metre

    @property
    def semi_minor_axis(self):
        return self._crs.ellipsoid.semi_minor_metre

    @property
    def inverse_flattening(self):
        return self._crs.ellipsoid.inverse_flattening

    @property
    def geographic(self) -> bool:
        return self._crs.is_geographic

    @property
    def projected(self) -> bool:
        return self._crs.is_projected

    @property
    def dimensions(self) -> Tuple[str, str]:
        """
        List of dimension names of the CRS.
        The ordering of the names is intended to reflect the `numpy` array axis order of the loaded raster.
        """
        if self.geographic:
            return 'latitude', 'longitude'

        if self.projected:
            return 'y', 'x'

        raise ValueError('Neither projected nor geographic')  # pragma: no cover

    @property
    def units(self) -> Tuple[str, str]:
        """
        List of dimension units of the CRS.
        The ordering of the units is intended to reflect the `numpy` array axis order of the loaded raster.
        """
        if self.geographic:
            return 'degrees_north', 'degrees_east'

        if self.projected:
            x, y = self._crs.axis_info
            return x.unit_name, y.unit_name

        raise ValueError('Neither projected nor geographic')  # pragma: no cover

    def __str__(self) -> str:
        return self.crs_str

    def __hash__(self):
        return hash(self.to_wkt())

    def __repr__(self) -> str:
        return "CRS('%s')" % self.crs_str

    def __eq__(self, other: SomeCRS) -> bool:
        if not isinstance(other, CRS):
            try:
                other = CRS(other)
            except Exception:
                return False

        if self._crs is other._crs:
            return True

        if self.epsg is not None and other.epsg is not None:
            return self.epsg == other.epsg

        return self._crs == other._crs

    def __ne__(self, other) -> bool:
        return not (self == other)

    def transformer_to_crs(self, other: 'CRS', always_xy=True) -> Callable[[float, float], Tuple[float, float]]:
        """
        Returns a function that maps x, y -> x', y' where x, y are coordinates in
        this stored either as scalars or ndarray objects and x', y' are the same
        points in the `other` CRS.
        """
        transform = _make_crs_transform(self._crs, other._crs, always_xy=always_xy)

        def result(x, y):
            rx, ry = transform(x, y)

            if not isinstance(rx, numpy.ndarray) or not isinstance(ry, numpy.ndarray):
                return (rx, ry)

            missing = numpy.isnan(rx) | numpy.isnan(ry)
            rx[missing] = numpy.nan
            ry[missing] = numpy.nan
            return (rx, ry)

        return result


class CRSMismatchError(ValueError):
    pass


def _norm_crs(crs: MaybeCRS) -> Optional[CRS]:
    if isinstance(crs, CRS):
        return crs
    if crs is None:
        return None
    return CRS(crs)


def wrap_shapely(method):
    """
    Takes a method that expects shapely geometry arguments
    and converts it to a method that operates on `Geometry`
    objects that carry their CRSs.
    """
    @functools.wraps(method, assigned=('__doc__', ))
    def wrapped(*args):
        first = args[0]
        for arg in args[1:]:
            if first.crs != arg.crs:
                raise CRSMismatchError((first.crs, arg.crs))

        result = method(*[arg.geom for arg in args])
        if isinstance(result, base.BaseGeometry):
            return Geometry(result, first.crs)
        return result
    return wrapped


def force_2d(geojson):
    assert 'type' in geojson
    assert 'coordinates' in geojson

    def is_scalar(x):
        return isinstance(x, (int, float))

    def go(x):
        if is_scalar(x):
            return x

        if isinstance(x, Sequence):
            if all(is_scalar(y) for y in x):
                return x[:2]
            return [go(y) for y in x]

        raise ValueError('invalid coordinate {}'.format(x))

    return {'type': geojson['type'],
            'coordinates': go(geojson['coordinates'])}


def densify(line, distance):
    """
    Adds points so they are at most `distance` apart.
    """
    if distance <= 0.0 or distance >= line.length:
        return line

    coords = list(line.coords)
    new_coords = [coords[0]]
    for start, end in zip(coords[:-1], coords[1:]):
        segment = geometry.LineString([start, end])
        while segment.length > distance:
            new_point = segment.interpolate(distance)
            segment = geometry.LineString([new_point, end])
            new_coords.append(new_point.coords[0])
        new_coords.append(end)

    return type(line)(new_coords)


class Geometry:
    """
    2D Geometry with CRS

    Instantiate with a GeoJSON structure

    If 3D coordinates are supplied, they are converted to 2D by dropping the Z points.
    """

    def __init__(self, geom: base.BaseGeometry, crs: MaybeCRS = None):
        crs = _norm_crs(crs)

        self.crs = crs
        if isinstance(geom, base.BaseGeometry):
            self.geom = geom
        else:
            self.geom = geometry.shape(force_2d(geom))

    @wrap_shapely
    def contains(self, other: 'Geometry') -> bool:
        return self.contains(other)

    @wrap_shapely
    def crosses(self, other: 'Geometry') -> bool:
        return self.crosses(other)

    @wrap_shapely
    def disjoint(self, other: 'Geometry') -> bool:
        return self.disjoint(other)

    @wrap_shapely
    def intersects(self, other: 'Geometry') -> bool:
        return self.intersects(other)

    @wrap_shapely
    def touches(self, other: 'Geometry') -> bool:
        return self.touches(other)

    @wrap_shapely
    def within(self, other: 'Geometry') -> bool:
        return self.within(other)

    @wrap_shapely
    def overlaps(self, other: 'Geometry') -> bool:
        return self.overlaps(other)

    @wrap_shapely
    def difference(self, other: 'Geometry') -> 'Geometry':
        return self.difference(other)

    @wrap_shapely
    def intersection(self, other: 'Geometry') -> 'Geometry':
        return self.intersection(other)

    @wrap_shapely
    def symmetric_difference(self, other: 'Geometry') -> 'Geometry':
        return self.symmetric_difference(other)

    @wrap_shapely
    def union(self, other: 'Geometry') -> 'Geometry':
        return self.union(other)

    @property
    def type(self) -> str:
        return self.geom.type

    @property
    def is_empty(self) -> bool:
        return self.geom.is_empty

    @property
    def is_valid(self) -> bool:
        return self.geom.is_valid

    @property
    def boundary(self) -> 'Geometry':
        return Geometry(self.geom.boundary, self.crs)

    @property
    def centroid(self) -> 'Geometry':
        return Geometry(self.geom.centroid, self.crs)

    @property
    def coords(self) -> CoordList:
        return self.geom.coords

    @property
    def points(self) -> CoordList:
        return self.coords

    @property
    def length(self) -> float:
        return self.geom.length

    @property
    def area(self) -> float:
        return self.geom.area

    @property
    def convex_hull(self) -> 'Geometry':
        return Geometry(self.geom.convex_hull, self.crs)

    @property
    def envelope(self) -> BoundingBox:
        minx, miny, maxx, maxy = self.geom.bounds
        return BoundingBox(left=minx, right=maxx, bottom=miny, top=maxy)

    @property
    def boundingbox(self) -> BoundingBox:
        return self.envelope

    @property
    def wkt(self) -> str:
        return self.geom.wkt

    @property
    def __geo_interface__(self):
        return self.geom.__geo_interface__

    @property
    def json(self):
        return self.__geo_interface__

    def segmented(self, resolution: float) -> 'Geometry':
        """
        Possibly add more points to the geometry so that no edge is longer than `resolution`.
        """

        def segmentize_shapely(geom):
            if geom.type in ['Point', 'MultiPoint']:
                return geom

            if geom.type in ['GeometryCollection', 'MultiPolygon', 'MultiLineString']:
                return type(geom)([segmentize_shapely(g) for g in geom])

            if geom.type in ['LineString', 'LinearRing']:
                return densify(geom, resolution)

            if geom.type == 'Polygon':
                return geometry.Polygon(densify(geom.exterior, resolution),
                                        [densify(i, resolution) for i in geom.interiors])

            raise ValueError('unknown geometry type {}'.format(geom.type))  # pragma: no cover

        clone = geometry.shape(self.json)

        return Geometry(segmentize_shapely(clone), self.crs)

    def interpolate(self, distance: float) -> 'Geometry':
        """
        Returns a point distance units along the line or None if underlying
        geometry doesn't support this operation.
        """
        return Geometry(self.geom.interpolate(distance), self.crs)

    def buffer(self, distance: float, resolution: float = 30) -> 'Geometry':
        return Geometry(self.geom.buffer(distance, resolution=resolution), self.crs)

    def simplify(self, tolerance: float, preserve_topology: bool = True) -> 'Geometry':
        return Geometry(self.geom.simplify(tolerance, preserve_topology=preserve_topology), self.crs)

    def to_crs(self, crs: SomeCRS,
               resolution: Optional[float] = None,
               wrapdateline: bool = False) -> 'Geometry':
        """
        Convert geometry to a different Coordinate Reference System

        :param CRS crs: CRS to convert to
        :param float resolution: Subdivide the geometry such it has no segment longer then the given distance.
        :param bool wrapdateline: Attempt to gracefully handle geometry that intersects the dateline
                                  when converting to geographic projections.
                                  Currently only works in few specific cases (source CRS is smooth over the dateline).
        :rtype: Geometry
        """
        crs = _norm_crs(crs)
        if self.crs == crs:
            return self

        assert crs is not None
        if self.crs is None:
            raise ValueError("Cannot project geometries without CRS")

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        transform = self.crs.transformer_to_crs(crs)
        clone = geometry.shape(self.json)

        if wrapdateline and crs.geographic:
            rtransform = crs.transformer_to_crs(self.crs)
            clone = _chop_along_antimeridian(clone, transform, rtransform)

        seg = Geometry(clone, self.crs).segmented(resolution)
        return Geometry(ops.transform(transform, seg.geom), crs)

    def __iter__(self) -> Iterable['Geometry']:
        for geom in self.geom:
            yield Geometry(geom, self.crs)

    def __nonzero__(self) -> bool:
        return not self.is_empty

    def __bool__(self) -> bool:
        return not self.is_empty

    def __eq__(self, other: Any) -> bool:
        return (hasattr(other, 'crs') and self.crs == other.crs and
                hasattr(other, 'geom') and self.geom == other.geom)

    def __str__(self):
        return 'Geometry(%s, %r)' % (self.__geo_interface__, self.crs)

    def __repr__(self):
        return 'Geometry(%s, %s)' % (self.geom, self.crs)

    # Implement pickle/unpickle
    # It does work without these two methods, but gdal/ogr prints 'ERROR 1: Empty geometries cannot be constructed'
    # when unpickling, which is quite unpleasant.
    def __getstate__(self):
        return {'geom': self.json, 'crs': self.crs}

    def __setstate__(self, state):
        self.__init__(**state)


def _dist(x, y):
    return x*x + y*y


def _chop_along_antimeridian(geom, transform, rtransform):
    """
    attempt to cut the geometry along the dateline
    idea borrowed from TransformBeforeAntimeridianToWGS84 with minor mods...
    """
    minx, miny, maxx, maxy = geom.bounds

    midx, midy = (minx + maxx) / 2, (miny + maxy) / 2
    mid_lon, mid_lat = transform(midx, midy)

    eps = 1.0e-9
    if not _is_smooth_across_dateline(mid_lat, transform, rtransform, eps):
        return geom

    left_of_dt = geometry.LineString([(180 - eps, -90), (180 - eps, 90)])
    left_of_dt = ops.transform(rtransform, densify(left_of_dt, 1))

    if not left_of_dt.intersects(geom):
        return geom

    right_of_dt = geometry.LineString([(-180 + eps, -90), (-180 + eps, 90)])
    right_of_dt = ops.transform(rtransform, densify(right_of_dt, 1))

    poly1 = geometry.Polygon([(minx, maxy), (minx, miny)] + list(left_of_dt.coords) + [(minx, maxy)])
    poly2 = geometry.Polygon([(maxx, maxy), (maxx, miny)] + list(right_of_dt.coords) + [(maxx, maxy)])
    chopper = geometry.MultiPolygon([poly1, poly2])
    return geom.intersection(chopper)


def _is_smooth_across_dateline(mid_lat, transform, rtransform, eps):
    """
    test whether the CRS is smooth over the dateline
    idea borrowed from IsAntimeridianProjToWGS84 with minor mods...
    """
    left_of_dt_x, left_of_dt_y = rtransform(180-eps, mid_lat)
    right_of_dt_x, right_of_dt_y = rtransform(-180+eps, mid_lat)

    if _dist(right_of_dt_x-left_of_dt_x, right_of_dt_y-left_of_dt_y) > 1:
        return False

    left_of_dt_lon, left_of_dt_lat = transform(left_of_dt_x, left_of_dt_y)
    right_of_dt_lon, right_of_dt_lat = transform(right_of_dt_x, right_of_dt_y)
    if (_dist(left_of_dt_lon - 180 + eps, left_of_dt_lat - mid_lat) > 2 * eps or
            _dist(right_of_dt_lon + 180 - eps, right_of_dt_lat - mid_lat) > 2 * eps):
        return False

    return True


###########################################
# Helper constructor functions a la shapely
###########################################


def point(x: float, y: float, crs: MaybeCRS):
    """
    Create a 2D Point

    >>> point(10, 10, crs=None)
    Geometry(POINT (10 10), None)

    :rtype: Geometry
    """
    return Geometry({'type': 'Point', 'coordinates': (x, y)}, crs=crs)


def multipoint(coords: CoordList, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiPoint Geometry

    >>> multipoint([(10, 10), (20, 20)], None)
    Geometry(MULTIPOINT (10 10, 20 20), None)

    :param list coords: list of x,y coordinate tuples
    :rtype: Geometry
    """
    return Geometry({'type': 'MultiPoint', 'coordinates': coords}, crs=crs)


def line(coords: CoordList, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D LineString (Connected set of lines)

    >>> line([(10, 10), (20, 20), (30, 40)], None)
    Geometry(LINESTRING (10 10, 20 20, 30 40), None)

    :param list coords: list of x,y coordinate tuples
    :rtype: Geometry
    """
    return Geometry({'type': 'LineString', 'coordinates': coords}, crs=crs)


def multiline(coords: List[CoordList], crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiLineString (Multiple disconnected sets of lines)

    >>> multiline([[(10, 10), (20, 20), (30, 40)], [(50, 60), (70, 80), (90, 99)]], None)
    Geometry(MULTILINESTRING ((10 10, 20 20, 30 40), (50 60, 70 80, 90 99)), None)

    :param list coords: list of lists of x,y coordinate tuples
    :rtype: Geometry
    """
    return Geometry({'type': 'MultiLineString', 'coordinates': coords}, crs=crs)


def polygon(outer, crs: MaybeCRS, *inners) -> Geometry:
    """
    Create a 2D Polygon

    >>> polygon([(10, 10), (20, 20), (20, 10), (10, 10)], None)
    Geometry(POLYGON ((10 10, 20 20, 20 10, 10 10)), None)

    :param list coords: list of 2d x,y coordinate tuples
    :rtype: Geometry
    """
    return Geometry({'type': 'Polygon', 'coordinates': (outer, )+inners}, crs=crs)


def multipolygon(coords: List[List[CoordList]], crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiPolygon

    >>> multipolygon([[[(10, 10), (20, 20), (20, 10), (10, 10)]], [[(40, 10), (50, 20), (50, 10), (40, 10)]]], None)
    Geometry(MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((40 10, 50 20, 50 10, 40 10))), None)

    :param list coords: list of lists of x,y coordinate tuples
    :rtype: Geometry
    """
    return Geometry({'type': 'MultiPolygon', 'coordinates': coords}, crs=crs)


def box(left, bottom, right, top, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D Box (Polygon)

    >>> box(10, 10, 20, 20, None)
    Geometry(POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10)), None)
    """
    points = [(left, bottom), (left, top), (right, top), (right, bottom), (left, bottom)]
    return polygon(points, crs=crs)


def polygon_from_transform(width: float, height: float, transform: Affine, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D Polygon from an affine transform

    :param float width:
    :param float height:
    :param Affine transform:
    :param crs: CRS
    """
    points = [(0, 0), (0, height), (width, height), (width, 0), (0, 0)]
    transform.itransform(points)
    return polygon(points, crs=crs)


###########################################
# Multi-geometry operations
###########################################


def unary_union(geoms: Iterable[Geometry]) -> Optional[Geometry]:
    """
    compute union of multiple (multi)polygons efficiently
    """
    geoms = list(geoms)
    if len(geoms) == 0:
        return None

    first = geoms[0]
    crs = first.crs
    for g in geoms[1:]:
        if crs != g.crs:
            raise CRSMismatchError((crs, g.crs))

    return Geometry(ops.unary_union([g.geom for g in geoms]), crs)


def unary_intersection(geoms: Iterable[Geometry]) -> Geometry:
    """
    compute intersection of multiple (multi)polygons
    """
    return functools.reduce(Geometry.intersection, geoms)


def _align_pix(left, right, res, off):
    """
    >>> "%.2f %d" % _align_pix(20, 30, 10, 0)
    '20.00 1'
    >>> "%.2f %d" % _align_pix(20, 30.5, 10, 0)
    '20.00 1'
    >>> "%.2f %d" % _align_pix(20, 31.5, 10, 0)
    '20.00 2'
    >>> "%.2f %d" % _align_pix(20, 30, 10, 3)
    '13.00 2'
    >>> "%.2f %d" % _align_pix(20, 30, 10, -3)
    '17.00 2'
    >>> "%.2f %d" % _align_pix(20, 30, -10, 0)
    '30.00 1'
    >>> "%.2f %d" % _align_pix(19.5, 30, -10, 0)
    '30.00 1'
    >>> "%.2f %d" % _align_pix(18.5, 30, -10, 0)
    '30.00 2'
    >>> "%.2f %d" % _align_pix(20, 30, -10, 3)
    '33.00 2'
    >>> "%.2f %d" % _align_pix(20, 30, -10, -3)
    '37.00 2'
    """
    if res < 0:
        res = -res
        val = math.ceil((right - off) / res) * res + off
        width = max(1, int(math.ceil((val - left - 0.1 * res) / res)))
    else:
        val = math.floor((left - off) / res) * res + off
        width = max(1, int(math.ceil((right - val - 0.1 * res) / res)))
    return val, width


class GeoBox:
    """
    Defines the location and resolution of a rectangular grid of data,
    including it's :py:class:`CRS`.

    :param CRS crs: Coordinate Reference System
    :param affine.Affine affine: Affine transformation defining the location of the geobox
    """

    def __init__(self, width: int, height: int, affine: Affine, crs: MaybeCRS):
        assert is_affine_st(affine), "Only axis-aligned geoboxes are currently supported"
        self.width = width
        self.height = height
        self.affine = affine
        self.extent = polygon_from_transform(width, height, affine, crs=crs)

    @classmethod
    def from_geopolygon(cls,
                        geopolygon: Geometry,
                        resolution: Tuple[float, float],
                        crs: MaybeCRS = None,
                        align: Optional[Tuple[float, float]] = None) -> 'GeoBox':
        """
        :param resolution: (y_resolution, x_resolution)
        :param CRS crs: CRS to use, if different from the geopolygon
        :param (float,float) align: Align geobox such that point 'align' lies on the pixel boundary.
        """
        align = align or (0.0, 0.0)
        assert 0.0 <= align[1] <= abs(resolution[1]), "X align must be in [0, abs(x_resolution)] range"
        assert 0.0 <= align[0] <= abs(resolution[0]), "Y align must be in [0, abs(y_resolution)] range"

        if crs is None:
            crs = geopolygon.crs
        else:
            geopolygon = geopolygon.to_crs(crs)

        bounding_box = geopolygon.boundingbox
        offx, width = _align_pix(bounding_box.left, bounding_box.right, resolution[1], align[1])
        offy, height = _align_pix(bounding_box.bottom, bounding_box.top, resolution[0], align[0])
        affine = (Affine.translation(offx, offy) * Affine.scale(resolution[1], resolution[0]))
        return GeoBox(crs=crs, affine=affine, width=width, height=height)

    def buffered(self, ybuff, xbuff) -> 'GeoBox':
        """
        Produce a tile buffered by ybuff, xbuff (in CRS units)
        """
        by, bx = (_round_to_res(buf, res) for buf, res in zip((ybuff, xbuff), self.resolution))
        affine = self.affine * Affine.translation(-bx, -by)

        return GeoBox(width=self.width + 2*bx,
                      height=self.height + 2*by,
                      affine=affine,
                      crs=self.crs)

    def __getitem__(self, roi) -> 'GeoBox':
        if isinstance(roi, int):
            roi = (slice(roi, roi+1), slice(None, None))

        if isinstance(roi, slice):
            roi = (roi, slice(None, None))

        if len(roi) > 2:
            raise ValueError('Expect 2d slice')

        if not all(s.step is None or s.step == 1 for s in roi):
            raise NotImplementedError('scaling not implemented, yet')

        roi = roi_normalise(roi, self.shape)
        ty, tx = [s.start for s in roi]
        h, w = roi_shape(roi)

        affine = self.affine * Affine.translation(tx, ty)

        return GeoBox(width=w, height=h, affine=affine, crs=self.crs)

    def __or__(self, other) -> 'GeoBox':
        """ A geobox that encompasses both self and other. """
        return geobox_union_conservative([self, other])

    def __and__(self, other) -> 'GeoBox':
        """ A geobox that is contained in both self and other. """
        return geobox_intersection_conservative([self, other])

    def is_empty(self) -> bool:
        return self.width == 0 or self.height == 0

    def __bool__(self) -> bool:
        return not self.is_empty()

    @property
    def transform(self) -> Affine:
        return self.affine

    @property
    def shape(self) -> Tuple[int, int]:
        return self.height, self.width

    @property
    def crs(self) -> Optional[CRS]:
        return self.extent.crs

    @property
    def dimensions(self) -> Tuple[str, str]:
        """
        List of dimension names of the GeoBox
        """
        crs = self.crs
        if crs is None:
            return ('y', 'x')
        return crs.dimensions

    @property
    def resolution(self) -> Tuple[float, float]:
        """
        Resolution in Y,X dimensions
        """
        return self.affine.e, self.affine.a

    @property
    def alignment(self) -> Tuple[float, float]:
        """
        Alignment of pixel boundaries in Y,X dimensions
        """
        return self.affine.yoff % abs(self.affine.e), self.affine.xoff % abs(self.affine.a)

    @property
    def coordinates(self) -> Dict[str, Coordinate]:
        """
        dict of coordinate labels
        """
        yres, xres = self.resolution
        yoff, xoff = self.affine.yoff, self.affine.xoff

        xs = numpy.arange(self.width) * xres + (xoff + xres / 2)
        ys = numpy.arange(self.height) * yres + (yoff + yres / 2)

        units = self.crs.units if self.crs is not None else ('1', '1')

        return OrderedDict((dim, Coordinate(labels, units, res))
                           for dim, labels, units, res in zip(self.dimensions, (ys, xs), units, (yres, xres)))

    def xr_coords(self, with_crs: Union[bool, str] = False) -> Dict[Hashable, xr.DataArray]:
        """ Dictionary of Coordinates in xarray format

            :param with_crs: If True include netcdf/cf style CRS Coordinate
            with default name 'spatial_ref', if with_crs is a string then treat
            the string as a name of the coordinate.

            Returns
            =======

            OrderedDict name:str -> xr.DataArray

            where names are either `y,x` for projected or `latitude, longitude` for geographic.

        """
        spatial_ref = "spatial_ref"
        if isinstance(with_crs, str):
            spatial_ref = with_crs
            with_crs = True

        attrs = {}
        coords = self.coordinates
        crs = self.crs
        if crs is not None:
            attrs['crs'] = str(crs)

        coords = dict((n, _coord_to_xr(n, c, **attrs))
                      for n, c in coords.items())  # type: Dict[Hashable, xr.DataArray]

        if with_crs and crs is not None:
            coords[spatial_ref] = _mk_crs_coord(crs, spatial_ref)

        return coords

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
        return "GeoBox({})".format(self.geographic_extent)

    def __repr__(self):
        return "GeoBox({width}, {height}, {affine!r}, {crs})".format(
            width=self.width,
            height=self.height,
            affine=self.affine,
            crs=self.extent.crs
        )

    def __eq__(self, other):
        if not isinstance(other, GeoBox):
            return False

        return (self.shape == other.shape
                and self.transform == other.transform
                and self.crs == other.crs)


def bounding_box_in_pixel_domain(geobox: GeoBox, reference: GeoBox) -> BoundingBox:
    """
    Returns the bounding box of `geobox` with respect to the pixel grid
    defined by `reference` when their coordinate grids are compatible,
    that is, have the same CRS, same pixel size and orientation, and
    are related by whole pixel translation,
    otherwise raises `ValueError`.
    """
    tol = 1.e-8

    if reference.crs != geobox.crs:
        raise ValueError("Cannot combine geoboxes in different CRSs")

    a, b, c, d, e, f, *_ = ~reference.affine * geobox.affine

    if not (numpy.isclose(a, 1) and numpy.isclose(b, 0) and is_almost_int(c, tol)
            and numpy.isclose(d, 0) and numpy.isclose(e, 1) and is_almost_int(f, tol)):
        raise ValueError("Incompatible grids")

    tx, ty = round(c), round(f)
    return BoundingBox(tx, ty, tx + geobox.width, ty + geobox.height)


def geobox_union_conservative(geoboxes: List[GeoBox]) -> GeoBox:
    """ Union of geoboxes. Fails whenever incompatible grids are encountered. """
    if len(geoboxes) == 0:
        raise ValueError("No geoboxes supplied")

    reference, *_ = geoboxes

    bbox = bbox_union(bounding_box_in_pixel_domain(geobox, reference=reference)
                      for geobox in geoboxes)

    affine = reference.affine * Affine.translation(*bbox[:2])

    return GeoBox(width=bbox.width, height=bbox.height, affine=affine, crs=reference.crs)


def geobox_intersection_conservative(geoboxes: List[GeoBox]) -> GeoBox:
    """
    Intersection of geoboxes. Fails whenever incompatible grids are encountered.
    """
    if len(geoboxes) == 0:
        raise ValueError("No geoboxes supplied")

    reference, *_ = geoboxes

    bbox = bbox_intersection(bounding_box_in_pixel_domain(geobox, reference=reference)
                             for geobox in geoboxes)

    # standardise empty geobox representation
    if bbox.left > bbox.right:
        bbox = BoundingBox(left=bbox.left, bottom=bbox.bottom, right=bbox.left, top=bbox.top)
    if bbox.bottom > bbox.top:
        bbox = BoundingBox(left=bbox.left, bottom=bbox.bottom, right=bbox.right, top=bbox.bottom)

    affine = reference.affine * Affine.translation(*bbox[:2])

    return GeoBox(width=bbox.width, height=bbox.height, affine=affine, crs=reference.crs)


def scaled_down_geobox(src_geobox, scaler: int):
    """Given a source geobox and integer scaler compute geobox of a scaled down image.

        Output geobox will be padded when shape is not a multiple of scaler.
        Example: 5x4, scaler=2 -> 3x2

        NOTE: here we assume that pixel coordinates are 0,0 at the top-left
              corner of a top-left pixel.

    """
    assert scaler > 1

    H, W = [X//scaler + (1 if X % scaler else 0)
            for X in src_geobox.shape]

    # Since 0,0 is at the corner of a pixel, not center, there is no
    # translation between pixel plane coords due to scaling
    A = src_geobox.transform * Affine.scale(scaler, scaler)

    return GeoBox(W, H, A, src_geobox.crs)


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


def intersects(a, b):
    return a.intersects(b) and not a.touches(b)


def bbox_union(bbs: Iterable[BoundingBox]) -> BoundingBox:
    """ Given a stream of bounding boxes compute enclosing BoundingBox
    """
    # pylint: disable=invalid-name

    L = B = float('+inf')
    R = T = float('-inf')

    for bb in bbs:
        l, b, r, t = bb
        L = min(l, L)
        B = min(b, B)
        R = max(r, R)
        T = max(t, T)

    return BoundingBox(L, B, R, T)


def bbox_intersection(bbs: Iterable[BoundingBox]) -> BoundingBox:
    """ Given a stream of bounding boxes compute the overlap BoundingBox
    """
    # pylint: disable=invalid-name

    L = B = float('-inf')
    R = T = float('+inf')

    for bb in bbs:
        l, b, r, t = bb
        L = max(l, L)
        B = max(b, B)
        R = min(r, R)
        T = min(t, T)

    return BoundingBox(L, B, R, T)


def _mk_crs_coord(crs: CRS, name: str = 'spatial_ref') -> xr.DataArray:
    if crs.projected:
        grid_mapping_name = crs._crs.to_cf().get('grid_mapping_name')
        if grid_mapping_name is None:
            grid_mapping_name = "??"
        grid_mapping_name = grid_mapping_name.lower()
    else:
        grid_mapping_name = "latitude_longitude"

    return xr.DataArray(numpy.asarray(0, 'int32'),
                        name=name,
                        dims=(),
                        attrs={'spatial_ref': crs.wkt,
                               'grid_mapping_name': grid_mapping_name})


def _coord_to_xr(name: str, c: Coordinate, **attrs) -> xr.DataArray:
    """ Construct xr.DataArray from named Coordinate object, this can then be used
        to define coordinates for xr.Dataset|xr.DataArray
    """
    attrs = dict(units=c.units,
                 resolution=c.resolution,
                 **attrs)
    return xr.DataArray(c.values,
                        coords={name: c.values},
                        dims=(name,),
                        attrs=attrs)
