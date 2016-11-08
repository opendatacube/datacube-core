import fiona
import numpy
import rasterio
import shapely.geometry
import xarray

import datacube


def transect(data, line, resolution, method='nearest', tolerance=None):
    """
    Extract line transect from data along geom

    :param xarray.Dataset data: data loaded via `Datacube.load`
    :param shapely.geometry.LineString line: line along which to extract the transect (CRS must match data.crs)
    :param float resolution: interval used to extract points along the line (in CRS units)
    :param str method: see xarray.Dataset.sel_points
    :param float tolerance: see xarray.Dataset.sel_points
    """
    dist = numpy.arange(0, int(line.length), resolution)
    points = zip(*[line.interpolate(d).coords[0] for d in dist])
    indexers = {
        data.crs.dimensions[0]: list(points[1]),
        data.crs.dimensions[1]: list(points[0])
    }
    return data.sel_points(xarray.DataArray(dist, name='distance', dims=['distance']),
                           method=method,
                           tolerance=tolerance,
                           **indexers)


def warp_geometry(geom, src_crs, dst_crs):
    """
    warp geometry from src_crs to dst_crs
    """
    return shapely.geometry.shape(rasterio.warp.transform_geom(src_crs, dst_crs, shapely.geometry.mapping(geom)))


def main():
    with fiona.open('line.shp') as shapes:
        line_crs = str(shapes.crs_wkt)
        line = shapely.geometry.shape(next(shapes)['geometry'])

    query = {
        'time': ('1990-01-01', '1991-01-01'),
        'x': (line.bounds[0], line.bounds[2]),
        'y': (line.bounds[1], line.bounds[3]),
        'crs': line_crs
    }

    dc = datacube.Datacube(app='line-trans-recipe')
    data = dc.load(product='ls5_nbar_albers', measurements=['red'], **query)

    line_albers = warp_geometry(line, query['crs'], data.crs.wkt)
    trans = transect(data, line_albers, abs(data.affine.a))
    trans.red.plot(x='distance', y='time')
