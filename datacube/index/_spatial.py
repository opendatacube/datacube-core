# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import cast

from odc.geo.crs import CRS
from odc.geo.geom import Geometry, box

from datacube.model import QueryDict, QueryField, Range
from datacube.utils.documents import JsonDict

H_SPATIAL_KEYS = ("lon", "longitude", "x")
V_SPATIAL_KEYS = ("lat", "latitude", "y")
COORDS_SPATIAL_KEYS: tuple[str, ...] = H_SPATIAL_KEYS + V_SPATIAL_KEYS

CRS_SPATIAL_KEYS = ("crs", "coordinate_reference_system")

# All of the above
NON_GEOPOLYGON_SPATIAL_KEYS: tuple[str, ...] = COORDS_SPATIAL_KEYS + CRS_SPATIAL_KEYS

# All of the above plus geopolygon
SPATIAL_KEYS: tuple[str, ...] = NON_GEOPOLYGON_SPATIAL_KEYS + ("geopolygon",)


def strip_all_spatial_fields_from_query(q: QueryDict) -> QueryDict:
    return {k: v for k, v in q.items() if k not in SPATIAL_KEYS}


def extract_geom_from_query(**q: QueryField) -> Geometry | None:
    """
    Utility method for index drivers supporting spatial indexes.

    Extract a Geometry from a dataset query.  Backwards compatible with old lat/lon style queries.

    :param q: A query dictionary
    :return: A polygon or multipolygon type Geometry.  None if no spatial query clauses.
    """
    geom: Geometry | None = None
    if q.get("geopolygon") is not None:
        # New geometry-style spatial query
        geom_term = cast(JsonDict | Geometry, q.get("geopolygon"))
        try:
            geom = Geometry(geom_term)
        except ValueError:
            # Can't convert to single Geometry. If it is an iterable of Geometries, return the union
            for term in geom_term:
                if geom is None:
                    geom = Geometry(term)
                else:
                    geom = geom.union(Geometry(term))
        for spatial_key in NON_GEOPOLYGON_SPATIAL_KEYS:
            if spatial_key in q:
                raise ValueError(
                    f"Cannot specify spatial key {spatial_key} AND geopolygon in the same query"
                )
        assert geom and geom.crs
    else:
        # Old lat/lon--style spatial query (or no spatial query)
        # TODO: latitude/longitude/x/y aliases for lat/lon
        #       Also some stuff is precalced at the api.core.Datacube level.
        #       THAT needs to offload to index driver when it can.
        lon = lat = None
        for coord in H_SPATIAL_KEYS:
            if coord in q:
                if lon is not None:
                    raise ValueError(
                        "Multiple horizontal coordinate ranges supplied: use only one of x, lon, longitude"
                    )
                lon = q.get(coord)
        for coord in V_SPATIAL_KEYS:
            if coord in q:
                if lat is not None:
                    raise ValueError(
                        "Multiple vertical coordinate ranges supplied: use only one of y, lat, latitude"
                    )
                lat = q.get(coord)
        crs_in = None
        for coord in CRS_SPATIAL_KEYS:
            if coord in q:
                if crs_in is not None:
                    raise ValueError("CRS is supplied twice")
                crs_in = q.get(coord)
        if crs_in is None:
            crs = CRS("epsg:4326")
        else:
            crs = CRS(crs_in)
        if lat is None and lon is None:
            # No spatial query
            return None

        # Old lat/lon--style spatial query
        # Normalise input to numeric ranges.
        delta = 0.000001
        if lat is None:
            lat = Range(begin=-90, end=90)
        elif isinstance(lat, int | float):
            lat = Range(lat - delta, lat + delta)
        else:
            # Treat as tuple
            begin, end = cast(tuple[int | float, int | float], lat)
            lat = Range(begin, end)

        if lon is None:
            lon = Range(begin=-180, end=180)
        elif isinstance(lon, int | float):
            lon = Range(lon - delta, lon + delta)
        else:
            # Treat as tuple
            begin, end = cast(tuple[int | float, int | float], lon)
            lon = Range(begin, end)
        geom = box(lon.begin, lat.begin, lon.end, lat.end, crs=crs)
    return geom
