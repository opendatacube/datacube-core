import fiona
import shapely.geometry
import rasterio
import rasterio.features

import datacube


def warp_geometry(geom, src_crs, dst_crs):
    """
    warp geometry from src_crs to dst_crs
    """
    return shapely.geometry.shape(rasterio.warp.transform_geom(src_crs, dst_crs, shapely.geometry.mapping(geom)))


def geometry_mask(geom, geobox, all_touched=False, invert=False):
    """
    rasterize geometry into a binary mask where pixels that overlap geometry are False
    """
    return rasterio.features.geometry_mask([geom],
                                           out_shape=geobox.shape,
                                           transform=geobox.affine,
                                           all_touched=all_touched,
                                           invert=invert)


def main():
    shape_file = 'my_shape_file.shp'
    with fiona.open(shape_file) as shapes:
        geom_crs = str(shapes.crs_wkt)
        geom = shapely.geometry.shape(next(shapes)['geometry'])

    query = {
        'time': ('1990-01-01', '1991-01-01'),
        'x': (geom.bounds[0], geom.bounds[2]),
        'y': (geom.bounds[1], geom.bounds[3]),
        'crs': geom_crs
    }

    dc = datacube.Datacube(config='/home/547/gxr547/config/prodcube.conf')
    data = dc.load(product='ls5_nbar_albers', measurements=['red'], **query)

    mask = geometry_mask(warp_geometry(geom, query['crs'], data.crs.wkt), data.geobox, invert=True)
    data = data.where(mask)
