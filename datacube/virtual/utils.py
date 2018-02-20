"""
Utility functions that mimic existing functionality in the core API.
Perhaps core can be refactored to use these to reduce code duplication.
"""
from __future__ import absolute_import

from datacube.api.query import query_geopolygon
from datacube.api.core import get_bounds
from datacube.utils import geometry, intersects


def select_datasets_inside_polygon(datasets, polygon):
    # essentially copied from Datacube.find_datasets
    for dataset in datasets:
        if polygon is None or intersects(polygon.to_crs(dataset.crs), dataset.extent):
            yield dataset


def output_geobox(datasets, grid_spec,
                  like=None, output_crs=None, resolution=None, align=None,
                  **query):
    # configure output geobox as in `datacube.Datacube.load`

    if like is not None:
        assert output_crs is None, "'like' and 'output_crs' are not supported together"
        assert resolution is None, "'like' and 'resolution' are not supported together"
        assert align is None, "'like' and 'align' are not supported together"
        return like.geobox

    if output_crs is not None:
        # user provided specifications
        if resolution is None:
            raise ValueError("Must specify 'resolution' when specifying 'output_crs'")
        crs = geometry.CRS(output_crs)
    else:
        # specification from grid_spec
        if grid_spec is None or grid_spec.crs is None:
            raise ValueError("Product has no default CRS. Must specify 'output_crs' and 'resolution'")
        crs = grid_spec.crs
        if resolution is None:
            if grid_spec.resolution is None:
                raise ValueError("Product has no default resolution. Must specify 'resolution'")
            resolution = grid_spec.resolution
            align = align or grid_spec.alignment  # TODO is the indentation wrong here?

    return geometry.GeoBox.from_geopolygon(query_geopolygon(**query) or get_bounds(datasets, crs),
                                           resolution, crs, align)


def product_definitions_from_index(index):
    return {product.name: product.definition
            for product in index.products.get_all()}
