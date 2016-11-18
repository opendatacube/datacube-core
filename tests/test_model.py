# coding=utf-8

import numpy
from datacube.model import BoundingBox, GeoPolygon, GeoBox, CRS, GridSpec


def test_geobox():
    points_list = [
        [(148.2697, -35.20111), (149.31254, -35.20111), (149.31254, -36.331431), (148.2697, -36.331431)],
        [(148.2697, 35.20111), (149.31254, 35.20111), (149.31254, 36.331431), (148.2697, 36.331431)],
        [(-148.2697, 35.20111), (-149.31254, 35.20111), (-149.31254, 36.331431), (-148.2697, 36.331431)],
        [(-148.2697, -35.20111), (-149.31254, -35.20111), (-149.31254, -36.331431), (-148.2697, -36.331431)],
        ]
    for points in points_list:
        polygon = GeoPolygon(points, CRS('EPSG:3577'))
        resolution = (-25, 25)
        geobox = GeoBox.from_geopolygon(polygon, resolution)

        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.left - polygon.boundingbox.left)
        assert abs(resolution[0]) > abs(geobox.extent.boundingbox.right - polygon.boundingbox.right)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.top - polygon.boundingbox.top)
        assert abs(resolution[1]) > abs(geobox.extent.boundingbox.bottom - polygon.boundingbox.bottom)

import pytest
from datacube.model import OverlappedGridSpec
with_either_gridspec = pytest.mark.parametrize('gridspec_class', [GridSpec, OverlappedGridSpec])

@with_either_gridspec
def test_grispec(gridspec_class):
    gs = gridspec_class(crs=CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.1, 0.1), origin=(10, 10))
    poly = GeoPolygon([(10, 12.2), (10.8, 13), (13, 10.8), (12.2, 10)], CRS('EPSG:4326'))
    cells = {index: geobox for index, geobox in list(gs.tiles_inside_geopolygon(poly))}
    assert set(cells.keys()) == {(0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1)}
    assert numpy.isclose(cells[(2, 0)].coordinates['longitude'].values, numpy.linspace(12.05, 12.95, num=10)).all()
    assert numpy.isclose(cells[(2, 0)].coordinates['latitude'].values, numpy.linspace(10.95, 10.05, num=10)).all()

@with_either_gridspec
def test_gridspec_upperleft(gridspec_class):
    """ Test to ensure grid indexes can be counted correctly from bottom left or top left
    """
    tile_bbox = BoundingBox(left=1934400.0, top=2414800.0, right=2084400.000, bottom=2264800.000)
    bbox = BoundingBox(left=1934615, top=2379460, right=1937615, bottom=2376460)
    # Upper left - validated against WELD product tile calculator
    # http://globalmonitoring.sdstate.edu/projects/weld/tilecalc.php
    gs = gridspec_class(crs=CRS('EPSG:5070'), tile_size=(-150000, 150000), resolution=(-30, 30),
                  origin=(3314800.0, -2565600.0))
    cells = {index: geobox for index, geobox in list(gs.tiles(bbox))}
    assert set(cells.keys()) == {(30, 6)}
    assert cells[(30, 6)].extent.boundingbox == tile_bbox

    gs = gridspec_class(crs=CRS('EPSG:5070'), tile_size=(150000, 150000), resolution=(-30, 30),
                  origin=(14800.0, -2565600.0))
    cells = {index: geobox for index, geobox in list(gs.tiles(bbox))}
    assert set(cells.keys()) == {(30, 15)}  # WELD grid spec has 21 vertical cells -- 21 - 6 = 15
    assert cells[(30, 15)].extent.boundingbox == tile_bbox


def test_overlapped_padding():
    """ Test padding works """
    #pass
    
def test_gridworkflow():
    from mock import MagicMock
    import datetime
    
    # ----- fake a datacube -----
    # e.g. let there be a dataset that coincides with a grid cell
    
    fakecrs = MagicMock()
    
    grid = 100 # spatial frequency in crs units
    pixel = 10 # square pixel linear dimension in crs units 
    gridcell = BoundingBox(left=grid, bottom=-grid, right=2*grid, top=-2*grid)
    # if cell(0,0) has lower left corner at grid origin,
    # and cell indices increase toward upper right,
    # then this will be cell(1,-2).
    gridspec = GridSpec(crs=fakecrs, tile_size=(grid, grid), resolution=(-pixel, pixel)) # e.g. product gridspec

    fakedataset = MagicMock()
    fakedataset.extent = GeoPolygon.from_boundingbox(gridcell, crs=fakecrs)
    fakedataset.center_time = t = datetime.datetime(2001,2,15)
    #fakedataset.local_path = ...    

    fakeindex = MagicMock()
    fakeindex.datasets.get_field_names.return_value = ['time'] # permit query on time
    fakeindex.datasets.search_eager.return_value = [fakedataset]

    # ------ test without padding ----
   
    from datacube.api.grid_workflow import GridWorkflow
    gw = GridWorkflow(fakeindex, gridspec)
    
    query = dict(product='fake_product_name', time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))
    
    # test backend : that it finds the expected cell/dataset
    assert gw.cell_observations(**query).keys() == [(1,-2)]
    
    # test frontend
    assert len(gw.list_tiles(**query)) == 1 
    
    # ------ introduce padding --------
    
    gw2 = GridWorkflow(fakeindex, gridspec, padding=2)
    assert len(gw2.list_tiles(**query)) == 9
    
    # ------ add another dataset (test grouping) -----

    # consider cell (2,-2)
    gridcell2 = BoundingBox(left=2*grid, bottom=-grid, right=3*grid, top=-2*grid)    
    fakedataset2 = MagicMock()
    fakedataset2.extent = GeoPolygon.from_boundingbox(gridcell2, crs=fakecrs)
    fakedataset2.center_time = t
    fakeindex.datasets.search_eager.return_value.append(fakedataset2)
    
    # test unpadded
    assert len(gw.list_tiles(**query)) == 2
    ti = numpy.datetime64(t,'ns')
    assert set(gw.list_tiles(**query).keys()) == {(1,-2,ti),(2,-2,ti)}
    
    # -------- inspect particular returned tile objects --------    
    
    # check the array shape
    
    tile = gw.list_tiles(**query)[1,-2,ti] # unpadded example
    assert grid/pixel == 10
    assert tile.shape == (1,10,10)
        
    padded_tile = gw2.list_tiles(**query)[1,-2,ti] # padded example
    assert grid/pixel + 2*gw2.grid_spec.padding == 14
    assert padded_tile.shape == (1,14,14)
    
    # count the sources
    
    assert len(       tile.sources.isel(time=0).item()) == 1    
    assert len(padded_tile.sources.isel(time=0).item()) == 2
       
    # check the geocoding
    
    assert tile.geobox.alignment == padded_tile.geobox.alignment
    assert tile.geobox.affine * (0,0) == padded_tile.geobox.affine * (2,2)
    assert tile.geobox.affine * (10,10) == padded_tile.geobox.affine * (10+2,10+2)

    # ------- check loading --------

    # GridWorkflow accesses the product_data API
    # to ultimately convert geobox,sources,measurements to xarray,
    # so only thing to check here is the call interface.
    
    measurement = dict(nodata=0, dtype=numpy.int)
    fakedataset.type.lookup_measurements.return_value = {'dummy': measurement}
    fakedataset2.type = fakedataset.type
    
    from mock import patch
    with patch('datacube.api.core.Datacube.product_data') as loader:
        
        data = GridWorkflow.load(tile)
        data2 = GridWorkflow.load(padded_tile)
    
    assert data is data2 is loader.return_value
    assert loader.call_count == 2
    
    for (args, kwargs), loadable in zip(loader.call_args_list, [tile, padded_tile]):
        assert args[0] is loadable.sources
        assert args[1] is loadable.geobox
        assert args[2][0] is measurement
        
    

def test_crs_equality():
    a = CRS("""PROJCS["unnamed",GEOGCS["Unknown datum based upon the custom spheroid",
               DATUM["Not specified (based on custom spheroid)",SPHEROID["Custom spheroid",6371007.181,0]],
               PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Sinusoidal"],
               PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],
               PARAMETER["false_northing",0],UNIT["Meter",1]]""")
    b = CRS("""PROJCS["unnamed",GEOGCS["unnamed ellipse",DATUM["unknown",SPHEROID["unnamed",6371007.181,0]],
               PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Sinusoidal"],
               PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],
               PARAMETER["false_northing",0],UNIT["Meter",1]]""")
    c = CRS('+a=6371007.181 +b=6371007.181 +units=m +y_0=0 +proj=sinu +lon_0=0 +no_defs +x_0=0')
    assert a == b
    assert a == c
    assert b == c

    assert a != CRS('EPSG:4326')

    a = CRS("""GEOGCS["GEOCENTRIC DATUM of AUSTRALIA",DATUM["GDA94",SPHEROID["GRS80",6378137,298.257222101]],
               PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]""")
    b = CRS("""GEOGCS["GRS 1980(IUGG, 1980)",DATUM["unknown",SPHEROID["GRS80",6378137,298.257222101]],
               PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]""")
    c = CRS('+proj=longlat +no_defs +ellps=GRS80')
    assert a == b
    assert a == c
    assert b == c
