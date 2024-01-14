# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" Geometric shapes and operations on them
"""
from warnings import warn

warn(
    'datacube.utils.geometry is now deprecated. Please use the odc-geo library instead.',
    DeprecationWarning,
    stacklevel=2)

from ._base import (  # noqa
    Coordinate,
    BoundingBox,
    CRSError,
    CRSMismatchError,
    CRS,
    MaybeCRS,
    SomeCRS,
    CoordList,
    Geometry,
    GeoBox,
    assign_crs,
    common_crs,
    bbox_union,
    bbox_intersection,
    crs_units_per_degree,
    geobox_union_conservative,
    geobox_intersection_conservative,
    intersects,
    scaled_down_geobox,
    point,
    multipoint,
    line,
    multiline,
    polygon,
    multipolygon,
    multigeom,
    box,
    sides,
    polygon_from_transform,
    unary_union,
    unary_intersection,
    lonlat_bounds,
    projected_lon,
    clip_lon180,
    chop_along_antimeridian,
    mid_longitude,
)

from .tools import (  # noqa
    is_affine_st,
    apply_affine,
    roi_boundary,
    roi_is_empty,
    roi_is_full,
    roi_intersect,
    roi_shape,
    roi_normalise,
    roi_from_points,
    roi_center,
    roi_pad,
    scaled_down_shape,
    scaled_down_roi,
    scaled_up_roi,
    decompose_rws,
    affine_from_pts,
    get_scale_at_point,
    native_pix_transform,
    compute_reproject_roi,
    split_translation,
    compute_axis_overlap,
    w_,
)

from ._warp import (  # noqa
    warp_affine,
    rio_reproject,
)

__all__ = [
    "Coordinate",
    "BoundingBox",
    "CRSError",
    "CRSMismatchError",
    "CRS",
    "MaybeCRS",
    "SomeCRS",
    "CoordList",
    "Geometry",
    "GeoBox",
    "assign_crs",
    "common_crs",
    "bbox_union",
    "bbox_intersection",
    "crs_units_per_degree",
    "geobox_union_conservative",
    "geobox_intersection_conservative",
    "intersects",
    "point",
    "multipoint",
    "line",
    "multiline",
    "polygon",
    "multipolygon",
    "multigeom",
    "box",
    "sides",
    "polygon_from_transform",
    "unary_union",
    "unary_intersection",
    "lonlat_bounds",
    "projected_lon",
    "clip_lon180",
    "chop_along_antimeridian",
    "mid_longitude",
    "is_affine_st",
    "apply_affine",
    "compute_axis_overlap",
    "roi_boundary",
    "roi_is_empty",
    "roi_is_full",
    "roi_intersect",
    "roi_shape",
    "roi_normalise",
    "roi_from_points",
    "roi_center",
    "roi_pad",
    "scaled_down_geobox",
    "scaled_down_shape",
    "scaled_down_roi",
    "scaled_up_roi",
    "decompose_rws",
    "affine_from_pts",
    "get_scale_at_point",
    "native_pix_transform",
    "compute_reproject_roi",
    "split_translation",
    "warp_affine",
    "rio_reproject",
    "w_",
]
