import functools
import itertools
import math
import array
import warnings
from collections import namedtuple, OrderedDict
from typing import Tuple, Iterable, List, Union, Optional, Any, Callable, Hashable, Dict, Iterator
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
SomeCRS = Union[str, 'CRS', _CRS, Dict[str, Any]]
MaybeCRS = Optional[SomeCRS]
CoordList = List[Tuple[float, float]]

# pylint: disable=too-many-lines


class BoundingBox(_BoundingBox):
    """Bounding box, defining extent in cartesian coordinates.
    """

    def buffered(self, ybuff: float, xbuff: float) -> 'BoundingBox':
        """
        Return a new BoundingBox, buffered in the x and y dimensions.

        :param ybuff: Y dimension buffering amount
        :param xbuff: X dimension buffering amount
        :return: new BoundingBox
        """
        return BoundingBox(left=self.left - xbuff, right=self.right + xbuff,
                           top=self.top + ybuff, bottom=self.bottom - ybuff)

    @property
    def span_x(self) -> float:
        return self.right - self.left

    @property
    def span_y(self) -> float:
        return self.top - self.bottom

    @property
    def width(self) -> int:
        return int(self.right - self.left)

    @property
    def height(self) -> int:
        return int(self.top - self.bottom)

    @property
    def range_x(self) -> Tuple[float, float]:
        return (self.left, self.right)

    @property
    def range_y(self) -> Tuple[float, float]:
        return (self.bottom, self.top)

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

    @staticmethod
    def from_xy(x: Tuple[float, float],
                y: Tuple[float, float]) -> 'BoundingBox':
        """ BoundingBox from x and y ranges

        :param x: (left, right)
        :param y: (bottom, top)
        """
        x1, x2 = sorted(x)
        y1, y2 = sorted(y)
        return BoundingBox(x1, y1, x2, y2)

    @staticmethod
    def from_points(p1: Tuple[float, float],
                    p2: Tuple[float, float]) -> 'BoundingBox':
        """ BoundingBox from 2 points
        :param p1: (x, y)
        :param p2: (x, y)
        """
        return BoundingBox.from_xy((p1[0], p2[0]),
                                   (p1[1], p2[1]))


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
    if isinstance(crs_spec, dict):
        crs_spec = _CRS.from_dict(crs_spec)

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

    __slots__ = ('_crs', '_epsg', '_str')

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
        self._str = crs_str

    def __getstate__(self):
        return {'crs_str': self._str}

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
        return self._str

    def __hash__(self) -> int:
        return hash(self.to_wkt())

    def __repr__(self) -> str:
        return "CRS('%s')" % self._str

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

    @property
    def proj(self) -> _CRS:
        """ Access proj.CRS object that this wraps
        """
        return self._crs

    @property
    def valid_region(self) -> Optional['Geometry']:
        """ Return valid region of this CRS.

            Bounding box in Lon/Lat as a 4 point Polygon in EPSG:4326.
            None if not defined
        """
        region = self._crs.area_of_use
        if region is None:
            return None
        x1, y1, x2, y2 = region.bounds
        return box(x1, y1, x2, y2, 'EPSG:4326')

    @property
    def crs_str(self) -> str:
        """ DEPRECATED
        """
        warnings.warn("Please use `str(crs)` instead of `crs.crs_str`", category=DeprecationWarning)
        return self._str

    def transformer_to_crs(self, other: 'CRS', always_xy=True) -> Callable[[Any, Any], Tuple[Any, Any]]:
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
    """
    Raised when geometry operation is attempted on geometries in different
    coordinate references.
    """
    pass


def _norm_crs(crs: MaybeCRS) -> Optional[CRS]:
    if isinstance(crs, CRS):
        return crs
    if crs is None:
        return None
    return CRS(crs)


def _norm_crs_or_error(crs: MaybeCRS) -> CRS:
    if isinstance(crs, CRS):
        return crs
    if crs is None:
        raise ValueError("Expect valid CRS")
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


def force_2d(geojson: Dict[str, Any]) -> Dict[str, Any]:
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


def densify(coords: CoordList, resolution: float) -> CoordList:
    """
    Adds points so they are at most `resolution` units apart.
    """
    d2 = resolution**2

    def short_enough(p1, p2):
        return (p1[0]**2 + p2[0]**2) < d2

    new_coords = [coords[0]]
    for p1, p2 in zip(coords[:-1], coords[1:]):
        if not short_enough(p1, p2):
            segment = geometry.LineString([p1, p2])
            segment_length = segment.length
            d = resolution
            while d < segment_length:
                pt, = segment.interpolate(d).coords
                new_coords.append(pt)
                d += resolution

        new_coords.append(p2)

    return new_coords


def _clone_shapely_geom(geom: base.BaseGeometry) -> base.BaseGeometry:
    return type(geom)(geom)


class Geometry:
    """
    2D Geometry with CRS

    Instantiate with a GeoJSON structure

    If 3D coordinates are supplied, they are converted to 2D by dropping the Z points.
    """

    def __init__(self,
                 geom: Union[base.BaseGeometry, Dict[str, Any], 'Geometry'],
                 crs: MaybeCRS = None):
        if isinstance(geom, Geometry):
            assert crs is None
            self.crs: Optional[CRS] = geom.crs
            self.geom: base.BaseGeometry = _clone_shapely_geom(geom.geom)
            return

        crs = _norm_crs(crs)
        self.crs = crs
        if isinstance(geom, base.BaseGeometry):
            self.geom = geom
        elif isinstance(geom, dict):
            self.geom = geometry.shape(force_2d(geom))
        else:
            raise ValueError(f'Unexpected type {type(geom)}')

    def clone(self) -> 'Geometry':
        return Geometry(self)

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

    @wrap_shapely
    def __and__(self, other: 'Geometry') -> 'Geometry':
        return self.__and__(other)

    @wrap_shapely
    def __or__(self, other: 'Geometry') -> 'Geometry':
        return self.__or__(other)

    @wrap_shapely
    def __xor__(self, other: 'Geometry') -> 'Geometry':
        return self.__xor__(other)

    @wrap_shapely
    def __sub__(self, other: 'Geometry') -> 'Geometry':
        return self.__sub__(other)

    def svg(self) -> str:
        return self.geom.svg()

    def _repr_svg_(self) -> str:
        return self.geom._repr_svg_()

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
    def exterior(self) -> 'Geometry':
        return Geometry(self.geom.exterior, self.crs)

    @property
    def interiors(self) -> List['Geometry']:
        return [Geometry(g, self.crs) for g in self.geom.interiors]

    @property
    def centroid(self) -> 'Geometry':
        return Geometry(self.geom.centroid, self.crs)

    @property
    def coords(self) -> CoordList:
        return list(self.geom.coords)

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
    def xy(self) -> Tuple[array.array, array.array]:
        return self.geom.xy

    @property
    def convex_hull(self) -> 'Geometry':
        return Geometry(self.geom.convex_hull, self.crs)

    @property
    def envelope(self) -> 'Geometry':
        return Geometry(self.geom.envelope, self.crs)

    @property
    def boundingbox(self) -> BoundingBox:
        minx, miny, maxx, maxy = self.geom.bounds
        return BoundingBox(left=minx, right=maxx, bottom=miny, top=maxy)

    @property
    def wkt(self) -> str:
        return self.geom.wkt

    @property
    def __array_interface__(self):
        return self.geom.__array_interface__

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

        def segmentize_shapely(geom: base.BaseGeometry) -> base.BaseGeometry:
            if geom.type in ['Point', 'MultiPoint']:
                return type(geom)(geom)  # clone without changes

            if geom.type in ['GeometryCollection', 'MultiPolygon', 'MultiLineString']:
                return type(geom)([segmentize_shapely(g) for g in geom])

            if geom.type in ['LineString', 'LinearRing']:
                return type(geom)(densify(list(geom.coords), resolution))

            if geom.type == 'Polygon':
                return geometry.Polygon(densify(list(geom.exterior.coords), resolution),
                                        [densify(list(i.coords), resolution) for i in geom.interiors])

            raise ValueError('unknown geometry type {}'.format(geom.type))  # pragma: no cover

        return Geometry(segmentize_shapely(self.geom), self.crs)

    def interpolate(self, distance: float) -> 'Geometry':
        """
        Returns a point distance units along the line.
        Raises TypeError if geometry doesn't support this operation.
        """
        return Geometry(self.geom.interpolate(distance), self.crs)

    def buffer(self, distance: float, resolution: float = 30) -> 'Geometry':
        return Geometry(self.geom.buffer(distance, resolution=resolution), self.crs)

    def simplify(self, tolerance: float, preserve_topology: bool = True) -> 'Geometry':
        return Geometry(self.geom.simplify(tolerance, preserve_topology=preserve_topology), self.crs)

    def transform(self, func) -> 'Geometry':
        """Applies func to all coordinates of Geometry and returns a new Geometry
           of the same type and in the same projection from the transformed coordinates.

           func maps x, y, and optionally z to output xp, yp, zp. The input
           parameters may be iterable types like lists or arrays or single values.
           The output shall be of the same type: scalars in, scalars out; lists
           in, lists out.
        """
        return Geometry(ops.transform(func, self.geom), self.crs)

    def _to_crs(self, crs: CRS) -> 'Geometry':
        assert self.crs is not None
        return Geometry(ops.transform(self.crs.transformer_to_crs(crs),
                                      self.geom), crs)

    def to_crs(self, crs: SomeCRS,
               resolution: Optional[float] = None,
               wrapdateline: bool = False) -> 'Geometry':
        """
        Convert geometry to a different Coordinate Reference System

        :param crs: CRS to convert to

        :param resolution: Subdivide the geometry such it has no segment longer then the given distance.
                           Defaults to 1 degree for geographic and 100km for projected. To disable
                           completely use Infinity float('+inf')

        :param wrapdateline: Attempt to gracefully handle geometry that intersects the dateline
                                  when converting to geographic projections.
                                  Currently only works in few specific cases (source CRS is smooth over the dateline).
        """
        crs = _norm_crs_or_error(crs)
        if self.crs == crs:
            return self

        if self.crs is None:
            raise ValueError("Cannot project geometries without CRS")

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        geom = self.segmented(resolution) if math.isfinite(resolution) else self

        eps = 1e-4
        if wrapdateline and crs.geographic:
            # TODO: derive precision from resolution by converting to degrees
            precision = 0.1
            chopped = chop_along_antimeridian(geom, precision)
            chopped_lonlat = chopped._to_crs(crs)
            return clip_lon180(chopped_lonlat, eps)

        return geom._to_crs(crs)

    def split(self, splitter: 'Geometry') -> Iterable['Geometry']:
        """ shapely.ops.split
        """
        if splitter.crs != self.crs:
            raise CRSMismatchError(self.crs, splitter.crs)

        for g in ops.split(self.geom, splitter.geom):
            yield Geometry(g, self.crs)

    def __iter__(self) -> Iterator['Geometry']:
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


def common_crs(geoms: Iterable[Geometry]) -> Optional[CRS]:
    """ Return CRS common across geometries, or raise CRSMismatchError
    """
    all_crs = [g.crs for g in geoms]
    if len(all_crs) == 0:
        return None
    ref = all_crs[0]
    for crs in all_crs[1:]:
        if crs != ref:
            raise CRSMismatchError()
    return ref


def projected_lon(crs: MaybeCRS,
                  lon: float,
                  lat: Tuple[float, float] = (-90.0, 90.0),
                  step: float = 1.0) -> Geometry:
    """ Project vertical line along some longitude into given CRS.
    """
    crs = _norm_crs_or_error(crs)
    yy = numpy.arange(lat[0], lat[1], step, dtype='float32')
    xx = numpy.full_like(yy, lon)
    tr = CRS('EPSG:4326').transformer_to_crs(crs)
    xx_, yy_ = tr(xx, yy)
    pts = [(float(x), float(y))
           for x, y in zip(xx_, yy_)
           if math.isfinite(x) and math.isfinite(y)]
    return line(pts, crs)


def clip_lon180(geom: Geometry, tol=1e-6) -> Geometry:
    """For every point in the ``lon=180|-180`` band clip to either 180 or -180
        180|-180 is decided based on where the majority of other points lie.

        NOTE: this will only do "right thing" for chopped geometries,
              expectation is that all the points are to one side of lon=180
              line, or in the the capture zone of lon=(+/-)180
    """
    thresh = 180 - tol

    def _clip_180(xx, clip):
        return [x if abs(x) < thresh else clip for x in xx]

    def _pick_clip(xx: List[float]):
        cc = 0
        for x in xx:
            if abs(x) < thresh:
                cc += (1 if x > 0 else -1)
        return 180 if cc >= 0 else -180

    def transformer(xx, yy):
        clip = _pick_clip(xx)
        return _clip_180(xx, clip), yy

    if geom.type.startswith('Multi'):
        return multigeom(g.transform(transformer) for g in geom)

    return geom.transform(transformer)


def chop_along_antimeridian(geom: Geometry,
                            precision: float = 0.1) -> Geometry:
    """
    Chop a geometry along the antimeridian

    :param geom: Geometry to maybe partition
    :param precision: in degrees
    :returns: either the same geometry if it doesn't intersect the antimeridian,
              or multi-geometry that has been split.
    """
    if geom.crs is None:
        raise ValueError("Expect geometry with CRS defined")

    l180 = projected_lon(geom.crs, 180, step=precision)
    if geom.intersects(l180):
        return multigeom(geom.split(l180))

    return geom


###########################################
# Helper constructor functions a la shapely
###########################################


def point(x: float, y: float, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D Point

    >>> point(10, 10, crs=None)
    Geometry(POINT (10 10), None)
    """
    return Geometry({'type': 'Point', 'coordinates': [float(x), float(y)]}, crs=crs)


def multipoint(coords: CoordList, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiPoint Geometry

    >>> multipoint([(10, 10), (20, 20)], None)
    Geometry(MULTIPOINT (10 10, 20 20), None)

    :param coords: list of x,y coordinate tuples
    """
    return Geometry({'type': 'MultiPoint', 'coordinates': coords}, crs=crs)


def line(coords: CoordList, crs: MaybeCRS) -> Geometry:
    """
    Create a 2D LineString (Connected set of lines)

    >>> line([(10, 10), (20, 20), (30, 40)], None)
    Geometry(LINESTRING (10 10, 20 20, 30 40), None)

    :param coords: list of x,y coordinate tuples
    """
    return Geometry({'type': 'LineString', 'coordinates': coords}, crs=crs)


def multiline(coords: List[CoordList], crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiLineString (Multiple disconnected sets of lines)

    >>> multiline([[(10, 10), (20, 20), (30, 40)], [(50, 60), (70, 80), (90, 99)]], None)
    Geometry(MULTILINESTRING ((10 10, 20 20, 30 40), (50 60, 70 80, 90 99)), None)

    :param coords: list of lists of x,y coordinate tuples
    """
    return Geometry({'type': 'MultiLineString', 'coordinates': coords}, crs=crs)


def polygon(outer, crs: MaybeCRS, *inners) -> Geometry:
    """
    Create a 2D Polygon

    >>> polygon([(10, 10), (20, 20), (20, 10), (10, 10)], None)
    Geometry(POLYGON ((10 10, 20 20, 20 10, 10 10)), None)

    :param coords: list of 2d x,y coordinate tuples
    """
    return Geometry({'type': 'Polygon', 'coordinates': (outer, )+inners}, crs=crs)


def multipolygon(coords: List[List[CoordList]], crs: MaybeCRS) -> Geometry:
    """
    Create a 2D MultiPolygon

    >>> multipolygon([[[(10, 10), (20, 20), (20, 10), (10, 10)]], [[(40, 10), (50, 20), (50, 10), (40, 10)]]], None)
    Geometry(MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((40 10, 50 20, 50 10, 40 10))), None)

    :param coords: list of lists of x,y coordinate tuples
    """
    return Geometry({'type': 'MultiPolygon', 'coordinates': coords}, crs=crs)


def box(left: float, bottom: float, right: float, top: float, crs: MaybeCRS) -> Geometry:
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

    :param width:
    :param height:
    :param transform:
    :param crs: CRS
    """
    points = [(0, 0), (0, height), (width, height), (width, 0), (0, 0)]
    transform.itransform(points)
    return polygon(points, crs=crs)


def sides(poly: Geometry) -> Iterable[Geometry]:
    """ Returns a sequence of Geometry[Line] objects.

        One for each side of the exterior ring of the input polygon.
    """
    XY = poly.exterior.points
    crs = poly.crs
    for p1, p2 in zip(XY[:-1], XY[1:]):
        yield line([p1, p2], crs)


def multigeom(geoms: Iterable[Geometry]) -> Geometry:
    """ Construct Multi{Polygon|LineString|Point}
    """
    geoms = [g for g in geoms]  # force into list
    src_type = {g.type for g in geoms}
    if len(src_type) > 1:
        raise ValueError("All Geometries must be of the same type")

    crs = common_crs(geoms)  # will raise if some differ
    raw_geoms = [g.geom for g in geoms]
    src_type = src_type.pop()
    if src_type == 'Polygon':
        return Geometry(geometry.MultiPolygon(raw_geoms), crs)
    elif src_type == 'Point':
        return Geometry(geometry.MultiPoint(raw_geoms), crs)
    elif src_type == 'LineString':
        return Geometry(geometry.MultiLineString(raw_geoms), crs)

    raise ValueError("Only understand Polygon|LineString|Point")


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


def _align_pix(left: float, right: float, res: float, off: float) -> Tuple[float, int]:
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

    :param crs: Coordinate Reference System
    :param affine: Affine transformation defining the location of the geobox
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
        :param crs: CRS to use, if different from the geopolygon
        :param align: Align geobox such that point 'align' lies on the pixel boundary.
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
    def geographic_extent(self) -> Geometry:
        """ GeoBox extent in EPSG:4326
        """
        if self.crs is None or self.crs.geographic:
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


def scaled_down_geobox(src_geobox: GeoBox, scaler: int) -> GeoBox:
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


def _round_to_res(value: float, res: float, acc: float = 0.1) -> int:
    res = abs(res)
    return int(math.ceil((value - 0.1 * res) / res))


def intersects(a: Geometry, b: Geometry) -> bool:
    """ Returns True if geometries intersect, else False
    """
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

    epsg = 0 if crs.epsg is None else crs.epsg

    return xr.DataArray(numpy.asarray(epsg, 'int32'),
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


def crs_units_per_degree(crs: SomeCRS,
                         lon: Union[float, Tuple[float, float]],
                         lat: float = 0,
                         step: float = 0.1) -> float:
    """ Compute number of CRS units per degree for a projected CRS at a given location
        in lon/lat.

        Location can be supplied as a tuple or as two arguments.

        Returns
        -------
        A floating number S such that `S*degrees -> meters`
    """
    if isinstance(lon, tuple):
        lon, lat = lon

    lon2 = lon + step
    if lon2 > 180:
        lon2 = lon - step

    ll = line([(lon, lat),
               (lon2, lat)],
              'EPSG:4326')
    xy = ll.to_crs(crs, resolution=math.inf)

    return xy.length / step


def lonlat_bounds(geom: Geometry,
                  mode: str = "safe",
                  resolution: Optional[float] = None) -> BoundingBox:
    """
    Return the bounding box of a geometry

    :param geom: Geometry in any projection
    :param mode: safe|quick
    :param resolution: If supplied will first segmentize input geometry to have no segment longer than ``resolution``,
                       this increases accuracy at the cost of computation
    """
    assert mode in ("safe", "quick")
    if geom.crs is None:
        raise ValueError("lonlat_bounds can only operate on Geometry with CRS defined")

    if geom.crs.geographic:
        return geom.boundingbox

    if geom.type in ('Polygon', 'MultiPolygon'):
        geom = geom.exterior

    if resolution is not None and math.isfinite(resolution):
        geom = geom.segmented(resolution)

    xx, yy = geom.to_crs('EPSG:4326', resolution=math.inf).xy
    xx_range = min(xx), max(xx)
    yy_range = min(yy), max(yy)

    if mode == "safe":
        # If range in Longitude is more than 180 then it's probably wrapped
        # around 180 (X-360 for X > 180), so we add back 360 but only for X<0
        # values. This only works if input geometry doesn't span more than half
        # a globe, so we need to check for that too, but this is not yet
        # implemented...

        span_x = xx_range[1] - xx_range[0]
        if span_x > 180:
            # TODO: check the case when input geometry spans >180 region.
            #       For now we assume "smaller" geometries not too close
            #       to poles.
            xx_ = [x + 360 if x < 0 else x for x in xx]
            xx_range_ = min(xx_), max(xx_)
            span_x_ = xx_range_[1] - xx_range_[0]
            if span_x_ < span_x:
                xx_range = xx_range_

    return BoundingBox.from_xy(xx_range, yy_range)


def assign_crs(xx: Union[xr.DataArray, xr.Dataset],
               crs: MaybeCRS = None,
               crs_coord_name: str = 'spatial_ref') -> Union[xr.Dataset, xr.DataArray]:
    """
    Assign CRS for a non-georegistered array or dataset.

    Returns a new object with CRS information populated.

    Can also be called without ``crs`` argument on data that already has CRS
    information but not in the format used by datacube, in this case CRS
    metadata will be restructured into a shape used by datacube. This format
    allows for better propagation of CRS information through various
    computations.

    .. code-block:: python

        xx = datacube.utils.geometry.assign_crs(xr.open_rasterio("some-file.tif"))
        print(xx.geobox)
        print(xx.astype('float32').geobox)


    :param xx:  Dataset or DataArray
    :param crs: CRS to assign, if omitted try to guess from attributes
    :param crs_coord_name: how to name crs corodinate (defaults to ``spatial_ref``)
    """
    if crs is None:
        geobox = getattr(xx, 'geobox', None)
        if geobox is None:
            raise ValueError("Failed to guess CRS for this object")
        crs = geobox.crs

    crs = _norm_crs_or_error(crs)
    crs_coord = _mk_crs_coord(crs, name=crs_coord_name)
    xx = xx.assign_coords({crs_coord.name: crs_coord})

    xx.attrs.update(grid_mapping=crs_coord_name)

    if isinstance(xx, xr.Dataset):
        for band in xx.data_vars.values():
            band.attrs.update(grid_mapping=crs_coord_name)

    return xx
