from __future__ import absolute_import, division

import functools
import cachetools

from osgeo import ogr, osr
from rasterio.coords import BoundingBox as _BoundingBox

from datacube import compat


class BoundingBox(_BoundingBox):  # pylint: disable=duplicate-bases
    def buffered(self, ybuff, xbuff):
        """
        Return a new BoundingBox, buffered in the x and y dimensions.

        :param ybuff: Y dimension buffering amount
        :param xbuff: X dimension buffering amount
        :return: new BoundingBox
        """
        return BoundingBox(left=self.left - xbuff, right=self.right + xbuff,
                           top=self.top + ybuff, bottom=self.bottom - ybuff)


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


###################################################
# Helper methods to build ogr.Geometry from geojson
###################################################


def _make_point(pt):
    geom = ogr.Geometry(ogr.wkbPoint)
    geom.AddPoint_2D(*pt)
    return geom


def _make_linear(type_, coordinates):
    geom = ogr.Geometry(type_)
    for pt in coordinates:
        geom.AddPoint_2D(*pt)
    return geom


def _make_multipoint(coordinates):
    return _make_linear(ogr.wkbMultiPoint, coordinates)


def _make_line(coordinates):
    return _make_linear(ogr.wkbLineString, coordinates)


def _make_multiline(coordinates):
    geom = ogr.Geometry(ogr.wkbMultiLineString)
    for line_coords in coordinates:
        geom.AddGeometryDirectly(_make_line(line_coords))
    return geom


def _make_polygon(coordinates):
    geom = ogr.Geometry(ogr.wkbPolygon)
    for ring_coords in coordinates:
        geom.AddGeometryDirectly(_make_linear(ogr.wkbLinearRing, ring_coords))
    return geom


def _make_multipolygon(coordinates):
    geom = ogr.Geometry(ogr.wkbMultiPolygon)
    for poly_coords in coordinates:
        geom.AddGeometryDirectly(_make_polygon(poly_coords))
    return geom


###################################################
# Helper methods to build ogr.Geometry from geojson
###################################################


def _get_coordinates(geom):
    """
    recursively extract coordinates from geometry
    """
    if geom.GetGeometryType() == ogr.wkbPoint:
        return geom.GetPoint_2D(0)
    if geom.GetGeometryType() in [ogr.wkbMultiPoint, ogr.wkbLineString, ogr.wkbLinearRing]:
        return geom.GetPoints()
    else:
        return [_get_coordinates(geom.GetGeometryRef(i)) for i in range(geom.GetGeometryCount())]


def _make_geom_from_ogr(geom, crs):
    result = Geometry.__new__(Geometry)
    result._geom = geom  # pylint: disable=protected-access
    result.crs = crs
    return result


#############################################
# Helper methods to wrap ogr.Geometry methods
#############################################


def _wrap_binary_bool(method):
    @functools.wraps(method, assigned=('__doc__', ))
    def wrapped(self, other):
        assert self.crs == other.crs
        return bool(method(self._geom, other._geom))  # pylint: disable=protected-access
    return wrapped


def _wrap_binary_geom(method):
    @functools.wraps(method, assigned=('__doc__', ))
    def wrapped(self, other):
        assert self.crs == other.crs
        return _make_geom_from_ogr(method(self._geom, other._geom), self.crs)  # pylint: disable=protected-access
    return wrapped


class Geometry(object):
    """
    Geometry with CRS

    :type _geom: ogr.Geometry
    :type crs: CRS
    """
    _geom_makers = {
        'Point': _make_point,
        'MultiPoint': _make_multipoint,
        'LineString': _make_line,
        'MultiLineString': _make_multiline,
        'Polygon': _make_polygon,
        'MultiPolygon': _make_multipolygon,
    }

    _geom_types = {
        ogr.wkbPoint: 'Point',
        ogr.wkbMultiPoint: 'MultiPoint',
        ogr.wkbLineString: 'LineString',
        ogr.wkbMultiLineString: 'MultiLineString',
        ogr.wkbPolygon: 'Polygon',
        ogr.wkbMultiPolygon: 'MultiPolygon',
    }

    contains = _wrap_binary_bool(ogr.Geometry.Contains)
    crosses = _wrap_binary_bool(ogr.Geometry.Crosses)
    disjoint = _wrap_binary_bool(ogr.Geometry.Disjoint)
    intersects = _wrap_binary_bool(ogr.Geometry.Intersects)
    touches = _wrap_binary_bool(ogr.Geometry.Touches)
    within = _wrap_binary_bool(ogr.Geometry.Within)

    difference = _wrap_binary_geom(ogr.Geometry.Difference)
    intersection = _wrap_binary_geom(ogr.Geometry.Intersection)
    symmetric_difference = _wrap_binary_geom(ogr.Geometry.SymDifference)
    union = _wrap_binary_geom(ogr.Geometry.Union)

    def __init__(self, geo, crs=None):
        self.crs = crs
        self._geom = Geometry._geom_makers[geo['type']](geo['coordinates'])

    @property
    def type(self):
        return Geometry._geom_types[self._geom.GetGeometryType()]

    @property
    def is_empty(self):
        return self._geom.IsEmpty()

    @property
    def is_valid(self):
        return self._geom.IsValid()

    @property
    def boundary(self):
        return _make_geom_from_ogr(self._geom.Boundary(), self.crs)

    @property
    def centroid(self):
        return _make_geom_from_ogr(self._geom.Centroid(), self.crs)

    @property
    def coords(self):
        return self._geom.GetPoints()

    @property
    def points(self):
        return self.coords

    @property
    def length(self):
        return self._geom.Length()

    @property
    def area(self):
        return self._geom.GetArea()

    @property
    def convex_hull(self):
        return _make_geom_from_ogr(self._geom.ConvexHull(), self.crs)

    @property
    def envelope(self):
        minx, maxx, miny, maxy = self._geom.GetEnvelope()
        return BoundingBox(left=minx, right=maxx, bottom=miny, top=maxy)

    @property
    def boundingbox(self):
        return self.envelope

    @property
    def wkt(self):
        return getattr(self._geom, 'ExportToIsoWkt', self._geom.ExportToWkt)()

    @property
    def __geo_interface__(self):
        return {
            'type': self.type,
            'coordinates': _get_coordinates(self._geom)
        }

    def segmented(self, resolution):
        clone = self._geom.Clone()
        clone.Segmentize(resolution)
        return _make_geom_from_ogr(clone, self.crs)

    def interpolate(self, distance):
        return _make_geom_from_ogr(self._geom.Value(distance), self.crs)

    def buffer(self, distance, quadsecs=30):
        return _make_geom_from_ogr(self._geom.Buffer(distance, quadsecs), self.crs)

    def simplify(self, tolerance):
        return _make_geom_from_ogr(self._geom.Simplify(tolerance), self.crs)

    def to_crs(self, crs, resolution=None):
        if self.crs == crs:
            return self

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs)  # pylint: disable=protected-access
        clone = self._geom.Clone()
        clone.Segmentize(resolution)
        clone.Transform(transform)

        return _make_geom_from_ogr(clone, crs)  # pylint: disable=protected-access

    def __iter__(self):
        for i in range(self._geom.GetGeometryCount()):
            yield _make_geom_from_ogr(self._geom.GetGeometryRef(i), self.crs)

    def __nonzero__(self):
        return not self.is_empty

    def __bool__(self):
        return not self.is_empty

    def __eq__(self, other):
        return self.crs == other.crs and self._geom.Equal(other._geom)  # pylint: disable=protected-access

    def __str__(self):
        return 'Geometry(%s, %r)' % (self.__geo_interface__, self.crs)

    def __repr__(self):
        return 'Geometry(%s, %s)' % (self._geom, self.crs)


###########################################
# Helper constructor functions a la shapely
###########################################


def point(x, y, crs):
    return Geometry({'type': 'Point', 'coordinates': (x, y)}, crs=crs)


def line(coords, crs):
    return Geometry({'type': 'LineString', 'coordinates': coords}, crs=crs)


def polygon(outer, crs, *inners):
    return Geometry({'type': 'Polygon', 'coordinates': (outer, )+inners}, crs=crs)


def box(left, bottom, right, top, crs):
    points = [(left, bottom), (left, top), (right, top), (right, bottom), (left, bottom)]
    return polygon(points, crs=crs)


###########################################
# Multi-geometry operations
###########################################


def unary_union(geoms):
    """
    compute union of multiple (multi)polygons efficiently
    """
    # pylint: disable=protected-access
    geom = ogr.Geometry(ogr.wkbMultiPolygon)
    crs = None
    for g in geoms:
        if crs:
            assert crs == g.crs
        else:
            crs = g.crs
        if g._geom.GetGeometryType() == ogr.wkbPolygon:
            geom.AddGeometry(g._geom)
        elif g._geom.GetGeometryType() == ogr.wkbMultiPolygon:
            for poly in g._geom:
                geom.AddGeometry(poly)
        else:
            raise ValueError('"%s" is not supported' % g.type)
    union = geom.UnionCascaded()
    return _make_geom_from_ogr(union, crs)


def unary_intersection(geoms):
    """
    compute intersection of multiple (multi)polygons
    """
    return functools.reduce(Geometry.intersection, geoms)
