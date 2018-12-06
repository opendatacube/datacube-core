import numpy as np
import osgeo
import pytest
import pickle

from datacube.utils import geometry
from datacube.utils.geometry import (
    decompose_rws,
    affine_from_pts,
    get_scale_at_point,
    native_pix_transform,
    scaled_down_geobox,
    compute_reproject_roi,
    roi_normalise,
    roi_shape,
    split_translation,
    is_affine_st,
    compute_axis_overlap,
    w_,
)
from datacube.model import GridSpec

epsg4326 = geometry.CRS('EPSG:4326')
epsg3577 = geometry.CRS('EPSG:3577')
epsg3857 = geometry.CRS('EPSG:3857')

AlbersGS = GridSpec(crs=epsg3577,
                    tile_size=(100000.0, 100000.0),
                    resolution=(-25, 25),
                    origin=(0.0, 0.0))


def mkA(rot=0, scale=(1, 1), shear=0, translation=(0, 0)):
    from affine import Affine
    return Affine.translation(*translation)*Affine.rotation(rot)*Affine.shear(shear)*Affine.scale(*scale)


def test_pickleable():
    poly = geometry.polygon([(10, 20), (20, 20), (20, 10), (10, 20)], crs=epsg4326)
    pickled = pickle.dumps(poly, pickle.HIGHEST_PROTOCOL)
    unpickled = pickle.loads(pickled)
    assert poly == unpickled


def test_geobox_simple():
    from affine import Affine
    t = geometry.GeoBox(4000, 4000,
                        Affine(0.00025, 0.0, 151.0, 0.0, -0.00025, -29.0),
                        epsg4326)

    expect_lon = np.asarray([151.000125,  151.000375,  151.000625,  151.000875,  151.001125,
                             151.001375,  151.001625,  151.001875,  151.002125,  151.002375])

    expect_lat = np.asarray([-29.000125, -29.000375, -29.000625, -29.000875, -29.001125,
                             -29.001375, -29.001625, -29.001875, -29.002125, -29.002375])
    expect_resolution = np.asarray([-0.00025, 0.00025])

    assert t.coordinates['latitude'].values.shape == (4000,)
    assert t.coordinates['longitude'].values.shape == (4000,)

    np.testing.assert_almost_equal(t.resolution, expect_resolution)
    np.testing.assert_almost_equal(t.coords['latitude'].values[:10], expect_lat)
    np.testing.assert_almost_equal(t.coords['longitude'].values[:10], expect_lon)

    assert (t == "some random thing") is False


def test_props():
    crs = epsg4326

    box1 = geometry.box(10, 10, 30, 30, crs=crs)
    assert box1
    assert box1.is_valid
    assert not box1.is_empty
    assert box1.area == 400.0
    assert box1.boundary.length == 80.0
    assert box1.centroid == geometry.point(20, 20, crs)

    triangle = geometry.polygon([(10, 20), (20, 20), (20, 10), (10, 20)], crs=crs)
    assert triangle.envelope == geometry.BoundingBox(10, 10, 20, 20)

    outer = next(iter(box1))
    assert outer.length == 80.0

    box1copy = geometry.box(10, 10, 30, 30, crs=crs)
    assert box1 == box1copy
    assert box1.convex_hull == box1copy  # NOTE: this might fail because of point order

    box2 = geometry.box(20, 10, 40, 30, crs=crs)
    assert box1 != box2

    bbox = geometry.BoundingBox(1, 0, 10, 13)
    assert bbox.width == 9
    assert bbox.height == 13

    pt = geometry.point(3, 4, crs)
    assert pt.json['coordinates'] == (3.0, 4.0)
    assert 'Point' in str(pt)
    assert bool(pt) is True
    assert pt.__nonzero__() is True


def test_tests():
    box1 = geometry.box(10, 10, 30, 30, crs=epsg4326)
    box2 = geometry.box(20, 10, 40, 30, crs=epsg4326)
    box3 = geometry.box(30, 10, 50, 30, crs=epsg4326)
    box4 = geometry.box(40, 10, 60, 30, crs=epsg4326)
    minibox = geometry.box(15, 15, 25, 25, crs=epsg4326)

    assert not box1.touches(box2)
    assert box1.touches(box3)
    assert not box1.touches(box4)

    assert box1.intersects(box2)
    assert box1.intersects(box3)
    assert not box1.intersects(box4)

    assert not box1.crosses(box2)
    assert not box1.crosses(box3)
    assert not box1.crosses(box4)

    assert not box1.disjoint(box2)
    assert not box1.disjoint(box3)
    assert box1.disjoint(box4)

    assert box1.contains(minibox)
    assert not box1.contains(box2)
    assert not box1.contains(box3)
    assert not box1.contains(box4)

    assert minibox.within(box1)
    assert not box1.within(box2)
    assert not box1.within(box3)
    assert not box1.within(box4)


def test_ops():
    box1 = geometry.box(10, 10, 30, 30, crs=epsg4326)
    box2 = geometry.box(20, 10, 40, 30, crs=epsg4326)
    box4 = geometry.box(40, 10, 60, 30, crs=epsg4326)

    union1 = box1.union(box2)
    assert union1.area == 600.0

    inter1 = box1.intersection(box2)
    assert bool(inter1)
    assert inter1.area == 200.0

    inter2 = box1.intersection(box4)
    assert not bool(inter2)
    assert inter2.is_empty
    # assert not inter2.is_valid  TODO: what's going on here?

    diff1 = box1.difference(box2)
    assert diff1.area == 200.0

    symdiff1 = box1.symmetric_difference(box2)
    assert symdiff1.area == 400.0

    # test segmented
    line = geometry.line([(0, 0), (0, 5), (10, 5)], epsg4326)
    line2 = line.segmented(2)
    assert line.crs is line2.crs
    assert line.length == line2.length
    assert len(line.coords) < len(line2.coords)

    # test interpolate
    pt = line.interpolate(1)
    assert pt.crs is line.crs
    assert pt.coords[0] == (0, 1)

    assert pt.interpolate(3) is None


def test_unary_union():
    box1 = geometry.box(10, 10, 30, 30, crs=epsg4326)
    box2 = geometry.box(20, 10, 40, 30, crs=epsg4326)
    box3 = geometry.box(30, 10, 50, 30, crs=epsg4326)
    box4 = geometry.box(40, 10, 60, 30, crs=epsg4326)

    union0 = geometry.unary_union([box1])
    assert union0 == box1

    union1 = geometry.unary_union([box1, box4])
    assert union1.type == 'MultiPolygon'
    assert union1.area == 2.0 * box1.area

    union2 = geometry.unary_union([box1, box2])
    assert union2.type == 'Polygon'
    assert union2.area == 1.5 * box1.area

    union3 = geometry.unary_union([box1, box2, box3, box4])
    assert union3.type == 'Polygon'
    assert union3.area == 2.5 * box1.area

    union4 = geometry.unary_union([union1, box2, box3])
    assert union4.type == 'Polygon'
    assert union4.area == 2.5 * box1.area

    with pytest.raises(ValueError):
        pt = geometry.point(6, 7, epsg4326)
        geometry.unary_union([pt, pt])


def test_unary_intersection():
    box1 = geometry.box(10, 10, 30, 30, crs=epsg4326)
    box2 = geometry.box(15, 10, 35, 30, crs=epsg4326)
    box3 = geometry.box(20, 10, 40, 30, crs=epsg4326)
    box4 = geometry.box(25, 10, 45, 30, crs=epsg4326)
    box5 = geometry.box(30, 10, 50, 30, crs=epsg4326)
    box6 = geometry.box(35, 10, 55, 30, crs=epsg4326)

    inter1 = geometry.unary_intersection([box1])
    assert bool(inter1)
    assert inter1 == box1

    inter2 = geometry.unary_intersection([box1, box2])
    assert bool(inter2)
    assert inter2.area == 300.0

    inter3 = geometry.unary_intersection([box1, box2, box3])
    assert bool(inter3)
    assert inter3.area == 200.0

    inter4 = geometry.unary_intersection([box1, box2, box3, box4])
    assert bool(inter4)
    assert inter4.area == 100.0

    inter5 = geometry.unary_intersection([box1, box2, box3, box4, box5])
    assert bool(inter5)
    assert inter5.type == 'LineString'
    assert inter5.length == 20.0

    inter6 = geometry.unary_intersection([box1, box2, box3, box4, box5, box6])
    assert not bool(inter6)
    assert inter6.is_empty


class TestCRSEqualityComparisons(object):
    def test_sinusoidal_comparison(self):
        a = geometry.CRS("""PROJCS["unnamed",
                                GEOGCS["Unknown datum based upon the custom spheroid",
                                DATUM["Not specified (based on custom spheroid)",
                                    SPHEROID["Custom spheroid",6371007.181,0]],
                           PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Sinusoidal"],
                           PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],
                           PARAMETER["false_northing",0],UNIT["Meter",1]]""")
        b = geometry.CRS("""PROJCS["unnamed",GEOGCS["unnamed ellipse",
                           DATUM["unknown",SPHEROID["unnamed",6371007.181,0]],
                           PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Sinusoidal"],
                           PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],
                           PARAMETER["false_northing",0],UNIT["Meter",1]]""")
        c = geometry.CRS('+a=6371007.181 +b=6371007.181 +units=m +y_0=0 +proj=sinu +lon_0=0 +no_defs +x_0=0')
        assert a == b
        assert a == c
        assert b == c

        assert a != epsg4326

    def test_grs80_comparison(self):
        a = geometry.CRS("""GEOGCS["GEOCENTRIC DATUM of AUSTRALIA",
                                DATUM["GDA94",SPHEROID["GRS80",6378137,298.257222101]],
                                PRIMEM["Greenwich",0],
                                UNIT["degree",0.0174532925199433]]""")
        b = geometry.CRS("""GEOGCS["GRS 1980(IUGG, 1980)",DATUM["unknown",SPHEROID["GRS80",6378137,298.257222101]],
                            PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]""")
        c = geometry.CRS('+proj=longlat +no_defs +ellps=GRS80')
        assert a == b
        assert a == c
        assert b == c

        assert a != epsg4326

    def test_australian_albers_comparison(self):
        a = geometry.CRS("""PROJCS["GDA94_Australian_Albers",GEOGCS["GCS_GDA_1994",
                            DATUM["Geocentric_Datum_of_Australia_1994",SPHEROID["GRS_1980",6378137,298.257222101]],
                            PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],
                            PROJECTION["Albers_Conic_Equal_Area"],
                            PARAMETER["standard_parallel_1",-18],
                            PARAMETER["standard_parallel_2",-36],
                            PARAMETER["latitude_of_center",0],
                            PARAMETER["longitude_of_center",132],
                            PARAMETER["false_easting",0],
                            PARAMETER["false_northing",0],
                            UNIT["Meter",1]]""")
        b = epsg3577

        assert a == b

        assert a != epsg4326


def test_no_epsg():
    c = geometry.CRS('+proj=longlat +no_defs +ellps=GRS80')
    b = geometry.CRS("""GEOGCS["GRS 1980(IUGG, 1980)",DATUM["unknown",SPHEROID["GRS80",6378137,298.257222101]],
                        PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]""")

    assert c.epsg is None
    assert b.epsg is None


def test_geobox():
    points_list = [
        [(148.2697, -35.20111), (149.31254, -35.20111), (149.31254, -36.331431), (148.2697, -36.331431)],
        [(148.2697, 35.20111), (149.31254, 35.20111), (149.31254, 36.331431), (148.2697, 36.331431)],
        [(-148.2697, 35.20111), (-149.31254, 35.20111), (-149.31254, 36.331431), (-148.2697, 36.331431)],
        [(-148.2697, -35.20111), (-149.31254, -35.20111), (-149.31254, -36.331431), (-148.2697, -36.331431),
         (148.2697, -35.20111)],
    ]
    for points in points_list:
        polygon = geometry.polygon(points, crs=epsg3577)
        resolution = (-25, 25)
        geobox = geometry.GeoBox.from_geopolygon(polygon, resolution)

        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.left - polygon.boundingbox.left)
        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.right - polygon.boundingbox.right)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.top - polygon.boundingbox.top)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.bottom - polygon.boundingbox.bottom)

    A = mkA(0, scale=(10, -10),
            translation=(-48800, -2983006))

    w, h = 512, 256
    gbox = geometry.GeoBox(w, h, A, epsg3577)

    assert gbox.shape == (h, w)
    assert gbox.transform == A
    assert gbox.extent.crs == gbox.crs
    assert gbox.geographic_extent.crs == epsg4326
    assert gbox.extent.boundingbox.height == h*10.0
    assert gbox.extent.boundingbox.width == w*10.0
    assert isinstance(str(gbox), str)
    assert 'EPSG:3577' in repr(gbox)

    assert geometry.GeoBox(1, 1, mkA(0), epsg4326).geographic_extent.crs == epsg4326

    g2 = gbox[:-10, :-20]
    assert g2.shape == (gbox.height - 10, gbox.width - 20)

    # step of 1 is ok
    g2 = gbox[::1, ::1]
    assert g2.shape == gbox.shape

    assert gbox[0].shape == (1, gbox.width)
    assert gbox[:3].shape == (3, gbox.width)

    with pytest.raises(NotImplementedError):
        gbox[::2, :]

    # too many slices
    with pytest.raises(ValueError):
        gbox[:1, :1, :]

    assert gbox.buffered(10, 0).shape == (gbox.height + 2*1, gbox.width)
    assert gbox.buffered(30, 20).shape == (gbox.height + 2*3, gbox.width + 2*2)


@pytest.mark.xfail(tuple(int(i) for i in osgeo.__version__.split('.')) < (2, 2),
                   reason='Fails under GDAL 2.1')
def test_wrap_dateline():
    sinus_crs = geometry.CRS("""PROJCS["unnamed",
                           GEOGCS["Unknown datum based upon the custom spheroid",
                           DATUM["Not specified (based on custom spheroid)", SPHEROID["Custom spheroid",6371007.181,0]],
                           PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],
                           PROJECTION["Sinusoidal"],
                           PARAMETER["longitude_of_center",0],
                           PARAMETER["false_easting",0],
                           PARAMETER["false_northing",0],
                           UNIT["Meter",1]]""")
    albers_crs = epsg3577
    geog_crs = epsg4326

    wrap = geometry.polygon([(12231455.716333, -5559752.598333),
                             (12231455.716333, -4447802.078667),
                             (13343406.236, -4447802.078667),
                             (13343406.236, -5559752.598333),
                             (12231455.716333, -5559752.598333)], crs=sinus_crs)
    wrapped = wrap.to_crs(geog_crs)
    assert wrapped.type == 'Polygon'
    wrapped = wrap.to_crs(geog_crs, wrapdateline=True)
    # assert wrapped.type == 'MultiPolygon' TODO: these cases are quite hard to implement.
    # hopefully GDAL's CutGeometryOnDateLineAndAddToMulti will be available through py API at some point

    wrap = geometry.polygon([(13343406.236, -5559752.598333),
                             (13343406.236, -4447802.078667),
                             (14455356.755667, -4447802.078667),
                             (14455356.755667, -5559752.598333),
                             (13343406.236, -5559752.598333)], crs=sinus_crs)
    wrapped = wrap.to_crs(geog_crs)
    assert wrapped.type == 'Polygon'
    wrapped = wrap.to_crs(geog_crs, wrapdateline=True)
    # assert wrapped.type == 'MultiPolygon' TODO: same as above

    wrap = geometry.polygon([(14455356.755667, -5559752.598333),
                             (14455356.755667, -4447802.078667),
                             (15567307.275333, -4447802.078667),
                             (15567307.275333, -5559752.598333),
                             (14455356.755667, -5559752.598333)], crs=sinus_crs)
    wrapped = wrap.to_crs(geog_crs)
    assert wrapped.type == 'Polygon'
    wrapped = wrap.to_crs(geog_crs, wrapdateline=True)
    # assert wrapped.type == 'MultiPolygon' TODO: same as above

    wrap = geometry.polygon([(3658653.1976781483, -4995675.379595791),
                             (4025493.916030875, -3947239.249752495),
                             (4912789.243100313, -4297237.125269571),
                             (4465089.861944263, -5313778.16975072),
                             (3658653.1976781483, -4995675.379595791)], crs=albers_crs)
    wrapped = wrap.to_crs(geog_crs)
    assert wrapped.type == 'Polygon'
    assert wrapped.intersects(geometry.line([(0, -90), (0, 90)], crs=geog_crs))
    wrapped = wrap.to_crs(geog_crs, wrapdateline=True)
    assert wrapped.type == 'MultiPolygon'
    assert not wrapped.intersects(geometry.line([(0, -90), (0, 90)], crs=geog_crs))


def test_3d_geometry_converted_to_2d_geometry():
    coordinates = [(115.8929714190001, -28.577007674999948, 0.0),
                   (115.90275429200005, -28.57698532699993, 0.0),
                   (115.90412631000004, -28.577577566999935, 0.0),
                   (115.90157040700001, -28.58521105999995, 0.0),
                   (115.89382838900008, -28.585473711999953, 0.0),
                   (115.8929714190001, -28.577007674999948, 0.0)]
    geom_3d = {'coordinates': [coordinates],
               'type': 'Polygon'}
    geom_2d = {'coordinates': [[(x, y) for x, y, z in coordinates]],
               'type': 'Polygon'}

    g_2d = geometry.Geometry(geom_2d)
    g_3d = geometry.Geometry(geom_3d)

    assert {2} == set(len(pt) for pt in g_3d.boundary.coords)  # All coordinates are 2D

    assert g_2d == g_3d  # 3D geometry has been converted to a 2D by dropping the Z axis


def test_3d_point_converted_to_2d_point():
    point = (-35.5029340, 145.9312455, 0.0)

    point_3d = {'coordinates': point,
                'type': 'Point'}
    point_2d = {'coordinates': (point[0], point[1]),
                'type': 'Point'}

    p_2d = geometry.Geometry(point_2d)
    p_3d = geometry.Geometry(point_3d)

    assert len(p_3d.coords[0]) == 2

    assert p_2d == p_3d


def test_crs():
    CRS = geometry.CRS

    crs = epsg3577
    assert crs.geographic is False
    assert crs.projected is True
    assert crs.dimensions == ('y', 'x')
    assert crs.epsg == 3577
    assert crs.units == ('metre', 'metre')
    assert isinstance(repr(crs), str)

    crs = epsg4326
    assert crs.geographic is True
    assert crs.projected is False
    assert crs.dimensions == ('latitude', 'longitude')
    assert crs.epsg == 4326

    crs2 = CRS(crs)
    assert crs2 == crs

    assert epsg3577 == epsg3577
    assert epsg3577 == 'EPSG:3577'
    assert (epsg3577 != epsg3577) is False
    assert (epsg3577 == epsg4326) is False
    assert (epsg3577 == 'EPSG:4326') is False
    assert epsg3577 != epsg4326
    assert epsg3577 != 'EPSG:4326'

    bad_crs = ['cupcakes',
               ('PROJCS["unnamed",'
                'GEOGCS["WGS 84", DATUM["WGS_1984", SPHEROID["WGS 84",6378137,298.257223563, AUTHORITY["EPSG","7030"]],'
                'AUTHORITY["EPSG","6326"]], PRIMEM["Greenwich",0, AUTHORITY["EPSG","8901"]],'
                'UNIT["degree",0.0174532925199433, AUTHORITY["EPSG","9122"]], AUTHORITY["EPSG","4326"]]]')]

    for bad in bad_crs:
        with pytest.raises(geometry.InvalidCRSError) as e:
            CRS('cupcakes')

        assert 'Not a valid CRS:' in str(e)


def test_polygon_path():
    from datacube.utils.geometry.tools import polygon_path

    pp = polygon_path([0, 1])
    assert pp.shape == (2, 5)
    assert set(pp.ravel()) == {0, 1}

    pp2 = polygon_path([0, 1], [0, 1])
    assert (pp2 == pp).all()

    pp = polygon_path([0, 1], [2, 3])
    assert set(pp[0].ravel()) == {0, 1}
    assert set(pp[1].ravel()) == {2, 3}


def test_gbox_boundary():
    from datacube.utils.geometry.tools import gbox_boundary
    import numpy as np

    xx = np.zeros((2, 6))

    bb = gbox_boundary(xx, 3)

    assert bb.shape == (4 + (3-2)*4, 2)
    assert set(bb.T[0]) == {0.0, 3.0, 6.0}
    assert set(bb.T[1]) == {0.0, 1.0, 2.0}


def test_geobox_scale_down():
    from datacube.utils.geometry import GeoBox, CRS

    crs = CRS('EPSG:3857')

    A = mkA(0, (111.2, 111.2), translation=(125671, 251465))
    for s in [2, 3, 4, 8, 13, 16]:
        gbox = GeoBox(233*s, 755*s, A, crs)
        gbox_ = scaled_down_geobox(gbox, s)

        assert gbox_.width == 233
        assert gbox_.height == 755
        assert gbox_.crs is crs
        assert gbox_.extent.contains(gbox.extent)
        assert gbox.extent.difference(gbox.extent).area == 0.0

    gbox = GeoBox(1, 1, A, crs)
    for s in [2, 3, 5]:
        gbox_ = scaled_down_geobox(gbox, 3)

        assert gbox_.shape == (1, 1)
        assert gbox_.crs is crs
        assert gbox_.extent.contains(gbox.extent)


def test_roi_tools():
    from datacube.utils.geometry import (
        roi_is_empty,
        roi_shape,
        roi_normalise,
        roi_boundary,
        roi_from_points,
        roi_center,
        roi_intersect,
        scaled_down_roi,
        scaled_up_roi,
        scaled_down_shape,
    )
    from numpy import s_

    assert roi_shape(s_[2:4, 3:4]) == (2, 1)
    assert roi_shape(s_[:4, :7]) == (4, 7)

    assert roi_is_empty(s_[:4, :5]) is False
    assert roi_is_empty(s_[1:1, :10]) is True
    assert roi_is_empty(s_[7:3, :10]) is True

    assert roi_is_empty(s_[:3]) is False
    assert roi_is_empty(s_[4:4]) is True

    roi = s_[0:8, 0:4]
    roi_ = scaled_down_roi(roi, 2)
    assert roi_shape(roi_) == (4, 2)
    assert scaled_down_roi(scaled_up_roi(roi, 3), 3) == roi

    assert scaled_down_shape(roi_shape(roi), 2) == roi_shape(scaled_down_roi(roi, 2))

    assert roi_shape(scaled_up_roi(roi, 10000, (40, 50))) == (40, 50)

    assert roi_normalise(s_[3:4], 40) == s_[3:4]
    assert roi_normalise(s_[:4], (40,)) == s_[0:4]
    assert roi_normalise(s_[:], (40,)) == s_[0:40]
    assert roi_normalise(s_[:-1], (3,)) == s_[0:2]
    assert roi_normalise(s_[-2:-1, :], (10, 20)) == s_[8:9, 0:20]
    assert roi_normalise(s_[-2:-1, :, 3:4], (10, 20, 100)) == s_[8:9, 0:20, 3:4]
    assert roi_center(s_[0:3]) == 1.5
    assert roi_center(s_[0:2, 0:6]) == (1, 3)

    roi = s_[0:2, 4:13]
    xy = roi_boundary(roi)

    assert xy.shape == (4, 2)
    assert roi_from_points(xy, (2, 13)) == roi

    assert roi_intersect(roi, roi) == roi
    assert roi_intersect(s_[0:3], s_[1:7]) == s_[1:3]
    assert roi_intersect(s_[0:3], (s_[1:7],)) == s_[1:3]
    assert roi_intersect((s_[0:3],), s_[1:7]) == (s_[1:3],)

    assert roi_intersect(s_[4:7, 5:6], s_[0:1, 7:8]) == s_[4:4, 6:6]


def test_split_translation():

    def verify(a, b):
        a = np.asarray(a)
        b = np.asarray(b)
        np.testing.assert_array_almost_equal(a, b)

    def tt(tx, ty, *expect):
        verify(split_translation((tx, ty)), expect)

    assert split_translation((1, 2)) == ((1, 2), (0, 0))
    assert split_translation((-1, -2)) == ((-1, -2), (0, 0))
    tt(1.3, 2.5, (1, 2), (0.3, 0.5))
    tt(1.1, 2.6, (1, 3), (0.1, -0.4))
    tt(-1.1, 2.8, (-1, 3), (-0.1, -0.2))
    tt(-1.9, 2.05, (-2, 2), (+0.1, 0.05))
    tt(-1.5, 2.45, (-1, 2), (-0.5, 0.45))


def get_diff(A, B):
    from math import sqrt
    return sqrt(sum((a-b)**2 for a, b in zip(A, B)))


def test_affine_checks():
    assert is_affine_st(mkA(scale=(1, 2), translation=(3, -10))) is True
    assert is_affine_st(mkA(scale=(1, -2), translation=(-3, -10))) is True
    assert is_affine_st(mkA(rot=0.1)) is False
    assert is_affine_st(mkA(shear=0.4)) is False


def test_affine_rsw():

    def run_test(a, scale, shear=0, translation=(0, 0), tol=1e-8):
        A = mkA(a, scale=scale, shear=shear, translation=translation)

        R, W, S = decompose_rws(A)

        assert get_diff(A, R*W*S) < tol
        assert get_diff(S, mkA(0, scale)) < tol
        assert get_diff(R, mkA(a, translation=translation)) < tol

    for a in (0, 12, 45, 33, 67, 89, 90, 120, 170):
        run_test(a, (1, 1))
        run_test(a, (0.5, 2))
        run_test(-a, (0.5, 2))

        run_test(a, (1, 2))
        run_test(-a, (1, 2))

        run_test(a, (2, -1))
        run_test(-a, (2, -1))

    run_test(0, (3, 4), 10)
    run_test(-33, (3, -1), 10, translation=(100, -333))


def test_fit():
    from random import uniform

    def run_test(A, n, tol=1e-5):
        X = [(uniform(0, 1), uniform(0, 1))
             for _ in range(n)]
        Y = [A*x for x in X]
        A_ = affine_from_pts(X, Y)

        assert get_diff(A, A_) < tol

    A = mkA(13, scale=(3, 4), shear=3, translation=(100, -3000))

    run_test(A, 3)
    run_test(A, 10)

    run_test(mkA(), 3)
    run_test(mkA(), 10)


def test_scale_at_point():
    def mk_transform(sx, sy):
        A = mkA(37, scale=(sx, sy), translation=(2127, 93891))

        def transofrom(pts):
            return [A*x for x in pts]

        return transofrom

    tol = 1e-4
    pt = (0, 0)
    for sx, sy in [(3, 4), (0.4, 0.333)]:
        tr = mk_transform(sx, sy)
        sx_, sy_ = get_scale_at_point(pt, tr)
        assert abs(sx - sx_) < tol
        assert abs(sy - sy_) < tol

        sx_, sy_ = get_scale_at_point(pt, tr, 0.1)
        assert abs(sx - sx_) < tol
        assert abs(sy - sy_) < tol


def test_pix_transform():
    pt = tuple([int(x/10)*10 for x in
                geometry.point(145, -35, epsg4326).to_crs(epsg3577).coords[0]])

    A = mkA(scale=(20, -20), translation=pt)

    src = geometry.GeoBox(1024, 512, A, epsg3577)
    dst = geometry.GeoBox.from_geopolygon(src.geographic_extent,
                                          (0.0001, -0.0001))

    tr = native_pix_transform(src, dst)

    pts_src = [(0, 0), (10, 20), (300, 200)]
    pts_dst = tr(pts_src)
    pts_src_ = tr.back(pts_dst)

    np.testing.assert_almost_equal(pts_src, pts_src_)
    assert tr.linear is None

    # check identity transform
    tr = native_pix_transform(src, src)

    pts_src = [(0, 0), (10, 20), (300, 200)]
    pts_dst = tr(pts_src)
    pts_src_ = tr.back(pts_dst)

    np.testing.assert_almost_equal(pts_src, pts_src_)
    np.testing.assert_almost_equal(pts_src, pts_dst)
    assert tr.linear is not None
    assert tr.back.linear is not None
    assert tr.back.back is tr

    # check scale only change
    tr = native_pix_transform(src, scaled_down_geobox(src, 2))
    pts_dst = tr(pts_src)
    pts_src_ = tr.back(pts_dst)

    assert tr.linear is not None
    assert tr.back.linear is not None
    assert tr.back.back is tr

    np.testing.assert_almost_equal(pts_dst,
                                   [(x/2, y/2) for (x, y) in pts_src])

    np.testing.assert_almost_equal(pts_src, pts_src_)


def test_compute_reproject_roi():
    src = AlbersGS.tile_geobox((15, -40))
    dst = geometry.GeoBox.from_geopolygon(src.extent.to_crs(epsg3857).buffer(10),
                                          resolution=src.resolution)

    rr = compute_reproject_roi(src, dst)

    assert rr.roi_src == np.s_[0:src.height, 0:src.width]
    assert 0 < rr.scale < 1
    assert rr.is_st is False
    assert rr.transform.linear is None
    assert rr.scale in rr.scale2

    # check pure translation case
    roi_ = np.s_[113:-100, 33:-10]
    rr = compute_reproject_roi(src, src[roi_])
    assert rr.roi_src == roi_normalise(roi_, src.shape)
    assert rr.scale == 1
    assert rr.is_st is True

    rr = compute_reproject_roi(src, src[roi_], padding=0, align=0)
    assert rr.roi_src == roi_normalise(roi_, src.shape)
    assert rr.scale == 1
    assert rr.scale2 == (1, 1)

    # check pure translation case
    roi_ = np.s_[113:-100, 33:-10]
    rr = compute_reproject_roi(src, src[roi_], align=256)

    assert rr.roi_src == np.s_[0:src.height, 0:src.width]
    assert rr.scale == 1

    roi_ = np.s_[113:-100, 33:-10]
    rr = compute_reproject_roi(src, src[roi_])

    assert rr.scale == 1
    assert roi_shape(rr.roi_src) == roi_shape(rr.roi_dst)
    assert roi_shape(rr.roi_dst) == src[roi_].shape


def test_window_from_slice():
    from numpy import s_

    assert w_[s_[:3, 4:5]] == ((0, 3), (4, 5))
    assert w_[s_[0:3, :5]] == ((0, 3), (0, 5))
    assert w_[list(s_[0:3, :5])] == ((0, 3), (0, 5))

    for roi in [s_[:3], s_[:3, :4, :5], 0]:
        with pytest.raises(ValueError):
            w_[roi]


def test_axis_overlap():
    s_ = np.s_

    # Source overlaps destination fully
    #
    # S: |<--------------->|
    # D:      |<----->|
    assert compute_axis_overlap(100, 20, 1, 10) == s_[10:30, 0:20]
    assert compute_axis_overlap(100, 20, 2, 10) == s_[10:50, 0:20]
    assert compute_axis_overlap(100, 20, 0.25, 10) == s_[10:15, 0:20]
    assert compute_axis_overlap(100, 20, -1, 80) == s_[60:80, 0:20]
    assert compute_axis_overlap(100, 20, -0.5, 50) == s_[40:50, 0:20]
    assert compute_axis_overlap(100, 20, -2, 90) == s_[50:90, 0:20]

    # Destination overlaps source fully
    #
    # S:      |<-------->|
    # D: |<----------------->|
    assert compute_axis_overlap(10, 100, 1, -10) == s_[0:10, 10:20]
    assert compute_axis_overlap(10, 100, 2, -10) == s_[0:10, 5:10]
    assert compute_axis_overlap(10, 100, 0.5, -10) == s_[0:10, 20:40]
    assert compute_axis_overlap(10, 100, -1, 11) == s_[0:10, 1:11]

    # Partial overlaps
    #
    # S: |<----------->|
    # D:     |<----------->|
    assert compute_axis_overlap(10, 10, 1, 3) == s_[3:10, 0:7]
    assert compute_axis_overlap(10, 15, 1, 3) == s_[3:10, 0:7]

    # S:     |<----------->|
    # D: |<----------->|
    assert compute_axis_overlap(10, 10, 1, -5) == s_[0:5, 5:10]
    assert compute_axis_overlap(50, 10, 1, -5) == s_[0:5, 5:10]

    # No overlaps
    # S: |<--->|
    # D:         |<--->|
    assert compute_axis_overlap(10, 10, 1, 11) == s_[10:10, 0:0]
    assert compute_axis_overlap(10, 40, 1, 11) == s_[10:10, 0:0]

    # S:         |<--->|
    # D: |<--->|
    assert compute_axis_overlap(10, 10, 1, -11) == s_[0:0, 10:10]
    assert compute_axis_overlap(40, 10, 1, -11) == s_[0:0, 10:10]
