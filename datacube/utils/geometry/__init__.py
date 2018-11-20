""" Geometric shapes and operations on them
"""

from ._base import (
    Coordinate,
    BoundingBox,
    CRSProjProxy,
    InvalidCRSError,
    CRS,
    Geometry,
    GeoBox,
    point,
    multipoint,
    line,
    multiline,
    polygon,
    multipolygon,
    box,
    polygon_from_transform,
    unary_union,
    unary_intersection,
)

__all__ = [
    "Coordinate",
    "BoundingBox",
    "CRSProjProxy",
    "InvalidCRSError",
    "CRS",
    "Geometry",
    "GeoBox",
    "point",
    "multipoint",
    "line",
    "multiline",
    "polygon",
    "multipolygon",
    "box",
    "polygon_from_transform",
    "unary_union",
    "unary_intersection",
]
