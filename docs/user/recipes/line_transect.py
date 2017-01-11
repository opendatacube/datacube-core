import fiona
import numpy
import xarray

import datacube
from datacube.utils import geometry


def transect(data, line, resolution, method='nearest', tolerance=None):
    """
    Extract line transect from data along geom

    :param xarray.Dataset data: data loaded via `Datacube.load`
    :param datacube.utils.Geometry line: line along which to extract the transect
    :param float resolution: interval used to extract points along the line (in data CRS units)
    :param str method: see xarray.Dataset.sel_points
    :param float tolerance: see xarray.Dataset.sel_points
    """
    assert line.type == 'LineString'
    line = line.to_crs(data.crs)
    dist = numpy.arange(0, line.length, resolution)
    points = [line.interpolate(d).coords[0] for d in dist]
    indexers = {
        data.crs.dimensions[0]: [p[1] for p in points],
        data.crs.dimensions[1]: [p[0] for p in points]
    }
    return data.sel_points(xarray.DataArray(dist, name='distance', dims=['distance']),
                           method=method,
                           tolerance=tolerance,
                           **indexers)


def main():
    with fiona.open('line.shp') as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
        first_geometry = next(shapes)['geometry']
        line = geometry.Geometry(first_geometry, crs=crs)

    query = {
        'time': ('1990-01-01', '1991-01-01'),
        'geopolygon': line
    }

    dc = datacube.Datacube(app='line-trans-recipe')
    data = dc.load(product='ls5_nbar_albers', measurements=['red'], **query)

    trans = transect(data, line, abs(data.affine.a))
    trans.red.plot(x='distance', y='time')
