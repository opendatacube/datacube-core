from collections import defaultdict

from osgeo import osr, ogr
from rasterio.coords import BoundingBox
from rasterio.warp import transform_bounds


def tile_datasets_with_storage_type(datasets, storage_type):
    """
    compute indexes of tiles covering the datasets, as well as
    which datasets comprise which tiles

    :type datasets:  list[datacube.model.Dataset]
    :type storage_type:  datacube.model.StorageType
    :rtype: dict[tuple[int, int], list[datacube.model.Dataset]]
    """
    bounds_override = storage_type.roi and _roi_to_bounds(storage_type.roi, storage_type.spatial_dimensions)
    return _grid_datasets(datasets, bounds_override, storage_type.crs, storage_type.tile_size)


def _roi_to_bounds(roi, dims):
    return BoundingBox(roi[dims[0]][0], roi[dims[1]][0], roi[dims[0]][1], roi[dims[1]][1])


def _grid_datasets(datasets, bounds_override, grid_proj, grid_size):
    tiles = defaultdict(list)
    for dataset in datasets:
        dataset_proj = dataset.crs
        dataset_bounds = dataset.bounds
        bounds = bounds_override or BoundingBox(*transform_bounds(dataset_proj, grid_proj, *dataset_bounds))

        for y in range(int(bounds.bottom // grid_size[1]), int(bounds.top // grid_size[1]) + 1):
            for x in range(int(bounds.left // grid_size[0]), int(bounds.right // grid_size[0]) + 1):
                tile_index = (x, y)
                if _check_intersect(tile_index, grid_size, grid_proj, dataset_bounds, dataset_proj):
                    tiles[tile_index].append(dataset)

    return tiles


def _check_intersect(tile_index, tile_size, tile_crs, dataset_bounds, dataset_crs):
    tile_sr = osr.SpatialReference()
    tile_sr.SetFromUserInput(tile_crs)
    dataset_sr = osr.SpatialReference()
    dataset_sr.SetFromUserInput(dataset_crs)
    transform = osr.CoordinateTransformation(tile_sr, dataset_sr)

    tile_poly = _poly_from_bounds(tile_index[0] * tile_size[0],
                                  tile_index[1] * tile_size[1],
                                  (tile_index[0] + 1) * tile_size[0],
                                  (tile_index[1] + 1) * tile_size[1],
                                  32)
    tile_poly.Transform(transform)

    return tile_poly.Intersects(_poly_from_bounds(*dataset_bounds))


def _poly_from_bounds(left, bottom, right, top, segments=None):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(left, bottom)
    ring.AddPoint(left, top)
    ring.AddPoint(right, top)
    ring.AddPoint(right, bottom)
    ring.AddPoint(left, bottom)
    if segments:
        ring.Segmentize(2 * (right + top - left - bottom) / segments)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly
