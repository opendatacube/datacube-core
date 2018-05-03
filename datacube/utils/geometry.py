from __future__ import absolute_import, division

import functools
import math
from collections import namedtuple, OrderedDict

import cachetools
import numpy
from affine import Affine
from osgeo import ogr, osr
from rasterio.coords import BoundingBox as _BoundingBox

from datacube import compat

Coordinate = namedtuple('Coordinate', ('values', 'units'))


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

    @property
    def width(self):
        return self.right - self.left

    @property
    def height(self):
        return self.top - self.bottom


class CRSProjProxy(object):
    def __init__(self, crs):
        self._crs = crs

    def __getattr__(self, item):
        return self._crs.GetProjParm(item)


class InvalidCRSError(ValueError):
    pass


@cachetools.cached({})
def _make_crs(crs_str):
    crs = osr.SpatialReference()

    # We don't bother checking the return code for errors, as the below ExportToProj4 does a more thorough job.
    crs.SetFromUserInput(crs_str)

    # Some will "validly" be parsed above, but return OGRERR_CORRUPT_DATA error when used here.
    # see the PROJCS["unnamed... doctest below for an example.
    if not crs.ExportToProj4():
        raise InvalidCRSError("Not a valid CRS: %r" % crs_str)

    if crs.IsGeographic() == crs.IsProjected():
        raise InvalidCRSError('CRS must be geographic or projected: %r' % crs_str)

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
    >>> # Due to Py 2 and 3 inconsistency in traceback formatting, we need to wrap the exceptions. Yuck.
    >>> try:
    ...    CRS('cupcakes')
    ... except InvalidCRSError as e:
    ...    print(e)
    Not a valid CRS: 'cupcakes'
    >>> # This one validly parses, but returns "Corrupt data" from gdal when used.
    >>> try:
    ...     CRS('PROJCS["unnamed",'
    ...     'GEOGCS["WGS 84", DATUM["WGS_1984", SPHEROID["WGS 84",6378137,298.257223563, AUTHORITY["EPSG","7030"]],'
    ...     'AUTHORITY["EPSG","6326"]], PRIMEM["Greenwich",0, AUTHORITY["EPSG","8901"]],'
    ...     'UNIT["degree",0.0174532925199433, AUTHORITY["EPSG","9122"]], AUTHORITY["EPSG","4326"]]]')
    ... except InvalidCRSError as e:
    ...    print(e)
    Not a valid CRS: 'PROJCS["...
    """

    def __init__(self, crs_str):
        """

        :param crs_str: string representation of a CRS, often an EPSG code like 'EPSG:4326'
        :raises: InvalidCRSError
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
        return None

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

        raise ValueError('Neither projected nor geographic')

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

        raise ValueError('Neither projected nor geographic')

    def __str__(self):
        return self.crs_str

    def __repr__(self):
        return "CRS('%s')" % self.crs_str

    def __eq__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        gdal_thinks_issame = self._crs.IsSame(other._crs) == 1  # pylint: disable=protected-access
        if gdal_thinks_issame:
            return True

        def to_canonincal_proj4(crs):
            return set(crs.ExportToProj4().split() + ['+wktext'])
        proj4_repr_is_same = to_canonincal_proj4(self._crs) == to_canonincal_proj4(other._crs)  # pylint: disable=protected-access
        return proj4_repr_is_same

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
    # Ignore the third dimension
    geom.AddPoint_2D(*pt[0:2])
    return geom


def _make_multi(type_, maker, coords):
    geom = ogr.Geometry(type_)
    for coord in coords:
        geom.AddGeometryDirectly(maker(coord))
    return geom


def _make_linear(type_, coordinates):
    geom = ogr.Geometry(type_)
    for pt in coordinates:
        # Ignore the third dimension
        geom.AddPoint_2D(*pt[0:2])
    return geom


def _make_multipoint(coordinates):
    return _make_multi(ogr.wkbMultiPoint, _make_point, coordinates)


def _make_line(coordinates):
    return _make_linear(ogr.wkbLineString, coordinates)


def _make_multiline(coordinates):
    return _make_multi(ogr.wkbMultiLineString, _make_line, coordinates)


def _make_polygon(coordinates):
    return _make_multi(ogr.wkbPolygon, functools.partial(_make_linear, ogr.wkbLinearRing), coordinates)


def _make_multipolygon(coordinates):
    return _make_multi(ogr.wkbMultiPolygon, _make_polygon, coordinates)


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
    2D Geometry with CRS

    Instantiate with a GeoJSON structure

    If 3D coordinates are supplied, they are converted to 2D by dropping the Z points.

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
    def json(self):
        return self.__geo_interface__

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

    def to_crs(self, crs, resolution=None, wrapdateline=False):
        """
        Convert geometry to a different Coordinate Reference System
        :param CRS crs: CRS to convert to
        :param float resolution: Subdivide the geometry such it has no segment longer then the given distance.
        :param bool wrapdateline: Attempt to gracefully handle geometry that intersects the dateline
                                  when converting to geographic projections.
                                  Currently only works in few specific cases (source CRS is smooth over the dateline).
        :rtype: Geometry
        """
        if self.crs == crs:
            return self

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs)  # pylint: disable=protected-access
        clone = self._geom.Clone()

        if wrapdateline and crs.geographic:
            rtransform = osr.CoordinateTransformation(crs._crs, self.crs._crs)  # pylint: disable=protected-access
            clone = _chop_along_antimeridian(clone, transform, rtransform)

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

    # Implement pickle/unpickle
    # It does work without these two methods, but gdal/ogr prints 'ERROR 1: Empty geometries cannot be constructed'
    # when unpickling, which is quite unpleasant.
    def __getstate__(self):
        return {'geo': self.json, 'crs': self.crs}

    def __setstate__(self, state):
        self.__init__(**state)


def _dist(x, y):
    return x*x + y*y


def _chop_along_antimeridian(geom, transform, rtransform):
    """
    attempt to cut the geometry along the dateline
    idea borrowed from TransformBeforeAntimeridianToWGS84 with minor mods...
    """
    minx, maxx, miny, maxy = geom.GetEnvelope()

    midx, midy = (minx+maxx)/2, (miny+maxy)/2
    mid_lon, mid_lat, _ = transform.TransformPoint(midx, midy)

    eps = 1.0e-9
    if not _is_smooth_across_dateline(mid_lat, transform, rtransform, eps):
        return geom

    left_of_dt = _make_line([(180 - eps, -90), (180 - eps, 90)])
    left_of_dt.Segmentize(1)
    left_of_dt.Transform(rtransform)

    if not left_of_dt.Intersects(geom):
        return geom

    right_of_dt = _make_line([(-180 + eps, -90), (-180 + eps, 90)])
    right_of_dt.Segmentize(1)
    right_of_dt.Transform(rtransform)

    chopper = _make_multipolygon([[[(minx, maxy), (minx, miny)] + left_of_dt.GetPoints() + [(minx, maxy)]],
                                  [[(maxx, maxy), (maxx, miny)] + right_of_dt.GetPoints() + [(maxx, maxy)]]])
    return geom.Intersection(chopper)


def _is_smooth_across_dateline(mid_lat, transform, rtransform, eps):
    """
    test whether the CRS is smooth over the dateline
    idea borrowed from IsAntimeridianProjToWGS84 with minor mods...
    """
    left_of_dt_x, left_of_dt_y, _ = rtransform.TransformPoint(180-eps, mid_lat)
    right_of_dt_x, right_of_dt_y, _ = rtransform.TransformPoint(-180+eps, mid_lat)

    if _dist(right_of_dt_x-left_of_dt_x, right_of_dt_y-left_of_dt_y) > 1:
        return False

    left_of_dt_lon, left_of_dt_lat, _ = transform.TransformPoint(left_of_dt_x, left_of_dt_y)
    right_of_dt_lon, right_of_dt_lat, _ = transform.TransformPoint(right_of_dt_x, right_of_dt_y)
    if (_dist(left_of_dt_lon - 180 + eps, left_of_dt_lat - mid_lat) > 2 * eps or
            _dist(right_of_dt_lon + 180 - eps, right_of_dt_lat - mid_lat) > 2 * eps):
        return False

    return True


###########################################
# Helper constructor functions a la shapely
###########################################


def point(x, y, crs):
    """
    >>> point(10, 10, crs=None)
    Geometry(POINT (10 10), None)
    """
    return Geometry({'type': 'Point', 'coordinates': (x, y)}, crs=crs)


def multipoint(coords, crs):
    """
    >>> multipoint([(10, 10), (20, 20)], None)
    Geometry(MULTIPOINT (10 10,20 20), None)
    """
    return Geometry({'type': 'MultiPoint', 'coordinates': coords}, crs=crs)


def line(coords, crs):
    """
    >>> line([(10, 10), (20, 20), (30, 40)], None)
    Geometry(LINESTRING (10 10,20 20,30 40), None)
    """
    return Geometry({'type': 'LineString', 'coordinates': coords}, crs=crs)


def multiline(coords, crs):
    """
    >>> multiline([[(10, 10), (20, 20), (30, 40)], [(50, 60), (70, 80), (90, 99)]], None)
    Geometry(MULTILINESTRING ((10 10,20 20,30 40),(50 60,70 80,90 99)), None)
    """
    return Geometry({'type': 'MultiLineString', 'coordinates': coords}, crs=crs)


def polygon(outer, crs, *inners):
    """
    >>> polygon([(10, 10), (20, 20), (20, 10), (10, 10)], None)
    Geometry(POLYGON ((10 10,20 20,20 10,10 10)), None)
    """
    return Geometry({'type': 'Polygon', 'coordinates': (outer, )+inners}, crs=crs)


def multipolygon(coords, crs):
    """
    >>> multipolygon([[[(10, 10), (20, 20), (20, 10), (10, 10)]], [[(40, 10), (50, 20), (50, 10), (40, 10)]]], None)
    Geometry(MULTIPOLYGON (((10 10,20 20,20 10,10 10)),((40 10,50 20,50 10,40 10))), None)
    """
    return Geometry({'type': 'MultiPolygon', 'coordinates': coords}, crs=crs)


def box(left, bottom, right, top, crs):
    """
    >>> box(10, 10, 20, 20, None)
    Geometry(POLYGON ((10 10,10 20,20 20,20 10,10 10)), None)
    """
    points = [(left, bottom), (left, top), (right, top), (right, bottom), (left, bottom)]
    return polygon(points, crs=crs)


def polygon_from_transform(width, height, transform, crs):
    points = [(0, 0), (0, height), (width, height), (width, 0), (0, 0)]
    transform.itransform(points)
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


class GeoBox(object):
    """
    Defines the location and resolution of a rectangular grid of data,
    including it's :py:class:`CRS`.

    :param geometry.CRS crs: Coordinate Reference System
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
        #: :rtype: geometry.Geometry
        self.extent = polygon_from_transform(width, height, affine, crs=crs)

    @classmethod
    def from_geopolygon(cls, geopolygon, resolution, crs=None, align=None):
        """
        :type geopolygon: geometry.Geometry
        :param resolution: (y_resolution, x_resolution)
        :param geometry.CRS crs: CRS to use, if different from the geopolygon
        :param (float,float) align: Align geobox such that point 'align' lies on the pixel boundary.
        :rtype: GeoBox
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
    def transform(self):
        return self.affine

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
