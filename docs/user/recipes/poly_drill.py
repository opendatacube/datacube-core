import fiona
import rasterio.features

import datacube
from datacube.utils import geometry


def geometry_mask(geoms, geobox, all_touched=False, invert=False):
    """
    Create a mask from shapes.

    By default, mask is intended for use as a
    numpy mask, where pixels that overlap shapes are False.
    :param list[Geometry] geoms: geometries to be rasterized
    :param datacube.utils.GeoBox geobox:
    :param bool all_touched: If True, all pixels touched by geometries will be burned in. If
                             false, only pixels whose center is within the polygon or that
                             are selected by Bresenham's line algorithm will be burned in.
    :param bool invert: If True, mask will be True for pixels that overlap shapes.
    """
    return rasterio.features.geometry_mask([geom.to_crs(geobox.crs) for geom in geoms],
                                           out_shape=geobox.shape,
                                           transform=geobox.affine,
                                           all_touched=all_touched,
                                           invert=invert)


def main():
    shape_file = 'my_shape_file.shp'
    with fiona.open(shape_file) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
        first_geometry = next(iter(shapes))['geometry']
        geom = geometry.Geometry(first_geometry, crs=crs)

    query = {
        'time': ('1990-01-01', '1991-01-01'),
        'geopolygon': geom
    }

    dc = datacube.Datacube(app='poly-drill-recipe')
    data = dc.load(product='ls5_nbar_albers', measurements=['red'], **query)

    mask = geometry_mask([geom], data.geobox, invert=True)
    data = data.where(mask)

    data.red.plot.imshow(col='time', col_wrap=5)
