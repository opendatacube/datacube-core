# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Tracking spatial indexes
"""

import logging
from collections.abc import Mapping
from threading import Lock

from antimeridian import fix_shape
from geoalchemy2 import Geometry
from odc.geo import CRS
from odc.geo import Geometry as Geom
from odc.geo.geom import multipolygon, polygon
from sqlalchemy import ForeignKey, delete, select, text
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, mapped_column
from sqlalchemy.sql.ddl import DropTable

from ._core import METADATA
from ._schema import Base, Dataset, SpatialIndex, SpatialIndexRecord, orm_registry
from .sql import SCHEMA_NAME

_LOG: logging.Logger = logging.getLogger(__name__)


# In theory, we could just use the SQLAlchemy registry for this, but it is not indexed
# in a useful way.
class SpatialIndexORMRegistry:
    """Threadsafe global registry of SpatialIndex ORM classes, indexed by EPSG/SRID code."""

    _registry: dict[int, type[SpatialIndex]] = {}
    _lock = Lock()

    def __init__(self) -> None:
        self._registry = self.__class__._registry
        self._lock = self.__class__._lock

    def _to_epsg(self, epsg_or_crs: CRS | int) -> int:
        """Utility method to convert an epsg_or_crs to an epsg."""
        if isinstance(epsg_or_crs, CRS):
            if epsg_or_crs.epsg is None:
                raise ValueError("CRS with no epsg number")
            return epsg_or_crs.epsg
        else:
            return epsg_or_crs

    def register(self, epsg_or_crs: CRS | int) -> bool:
        """Ensure that SpatialIndex ORM clss is registered for this EPSG/SRID"""
        epsg = self._to_epsg(epsg_or_crs)
        added = False
        with self._lock:
            if epsg not in self._registry:
                self._registry[epsg] = self._mint_new_spindex(epsg)
                added = True
        return added

    def get(self, epsg_or_crs: CRS | int) -> type[SpatialIndex] | None:
        """Retrieve the registered SpatialIndex ORM class"""
        epsg = self._to_epsg(epsg_or_crs)
        return self._registry.get(epsg)

    def _mint_new_spindex(self, epsg: int) -> type[SpatialIndex]:
        """
        Dynamically create a new ORM class for a EPSG/SRID.

        Note: Called within registry lock.
        """
        attributes = {
            "__tablename__": f"spatial_{epsg}",
            "__table_args__": (
                METADATA,
                {
                    "schema": SCHEMA_NAME,
                    "comment": "A product or dataset type, family of related datasets.",
                },
            ),
            "dataset_ref": mapped_column(
                postgres.UUID(as_uuid=True),
                ForeignKey(Dataset.id),
                primary_key=True,
                nullable=False,
                comment="The dataset being indexed",
            ),
            "extent": mapped_column(
                Geometry("MULTIPOLYGON", srid=epsg),
                nullable=False,
                comment="The extent of the dataset",
            ),
        }
        return type(f"SpatialIdx{epsg}", (SpatialIndex, Base), attributes)


def is_spindex_table_name(name: str) -> bool:
    bits = name.split("_")
    if len(bits) == 2:
        if bits[0] == "spatial":
            try:
                srid = int(bits[1])
                return srid > 0
            except ValueError:
                pass
    return False


def spindex_for_epsg(epsg: int) -> type[SpatialIndex]:
    """Return ORM class of a SpatialIndex for EPSG/SRID - dynamically creating if necessary"""
    sir = SpatialIndexORMRegistry()
    spindex = sir.get(epsg)
    if spindex is None:
        sir.register(epsg)
        spindex = sir.get(epsg)
        assert spindex is not None  # for type-checker
    return spindex


def crs_to_epsg(crs: CRS) -> int:
    if not str(crs).upper().startswith("EPSG:") and crs.epsg is None:
        raise ValueError("Non-EPSG-style CRS.")
    elif crs.epsg is not None:
        return crs.epsg
    else:
        return int(str(crs)[5:])


def spindex_for_crs(crs: CRS) -> type[SpatialIndex]:
    """Return ORM class of a SpatialIndex for CRS - dynamically creating if necessary"""
    try:
        return spindex_for_epsg(crs_to_epsg(crs))
    except ValueError:
        # Postgis identifies CRSs by a numeric "SRID" which is equivalent to EPSG number.
        raise ValueError(
            f"Cannot create a postgis spatial index for a non-EPSG-style CRS: {crs!s}"
        ) from None


def spindex_for_record(rec: SpatialIndexRecord) -> type[SpatialIndex]:
    """Convert a Record of a SpatialIndex created in a particular database to an ORM class"""
    return spindex_for_epsg(rec.srid)


def ensure_spindex(engine: Engine, sp_idx: type[SpatialIndex], crs_id: int) -> None:
    """Ensure a Spatial Index exists in a particular database."""
    with Session(engine) as session:
        results = session.execute(
            select(SpatialIndexRecord.srid).where(SpatialIndexRecord.srid == crs_id)
        )
        for _ in results:
            # SpatialIndexRecord exists - actual index assumed to exist too.
            return
        # SpatialIndexRecord doesn't exist - create the index table...
        orm_registry.metadata.create_all(engine, [sp_idx.__table__])  # type: ignore[attr-defined]
        # ... and add a SpatialIndexRecord
        session.add(SpatialIndexRecord(srid=crs_id, table_name=sp_idx.__tablename__))  # type: ignore[attr-defined]
        session.commit()
        session.flush()
        # Permissions Management
        for command in [
            # Read access to odc_user
            f"grant select on {SCHEMA_NAME}.{sp_idx.__tablename__} to odc_user;",  # type: ignore[attr-defined]
            # Insert access to odc_manage
            f"grant insert on {SCHEMA_NAME}.{sp_idx.__tablename__} to odc_manage;",  # type: ignore[attr-defined]
            # Full access to odc_admin
            f"grant all on {SCHEMA_NAME}.{sp_idx.__tablename__} to odc_admin;",  # type: ignore[attr-defined]
        ]:
            session.execute(text(command))
        # Grant statements in PostgreSQL behave like transactions, so commit them.
        session.commit()


def drop_spindex(engine: Engine, sp_idx: type[SpatialIndex], crs_id: int) -> bool:
    with Session(engine) as session:
        results = session.execute(
            select(SpatialIndexRecord).where(SpatialIndexRecord.srid == crs_id)
        )
        spidx_record = None
        for result in results:
            spidx_record = result[0]
            break
        record_del_result = False
        if spidx_record:
            del_res = session.execute(
                delete(SpatialIndexRecord).where(
                    SpatialIndexRecord.srid == spidx_record.srid
                )
            )
            record_del_result = del_res.rowcount == 1

        drop_res = session.execute(
            DropTable(sp_idx.__table__, if_exists=True)  # type: ignore[attr-defined]
        )
        drop_table_result = drop_res.rowcount == 1  # type: ignore[attr-defined]
        _LOG.warning(
            f"spindex record deleted: {record_del_result}   table dropped: {drop_table_result}"
        )

    return True


def spindexes(engine: Engine) -> Mapping[int, type[SpatialIndex]]:
    """
    Return a SRID-to-Spatial Index ORM class mapping for indexes that exist in a particular database.
    """
    out = {}
    with Session(engine) as session:
        results = session.execute(select(SpatialIndexRecord.srid))
        for result in results:
            epsg = int(result[0])
            spindex = spindex_for_epsg(epsg)
            out[epsg] = spindex
    return out


def promote_to_multipolygon(geom: Geom) -> Geom:
    # Assumes input is a polygon or multipolygon - does not work on lines or points
    if geom.geom_type == "MultiPolygon":
        return geom
    elif geom.geom_type == "Polygon":
        # Promote to multipolygon (is there a more elegant way to do this??)
        polycoords = [list(geom.geom.exterior.coords)]
        for interior in geom.geom.interiors:
            polycoords.append(list(interior.coords))
        geom = multipolygon([polycoords], crs=geom.crs)
        return geom
    else:
        raise ValueError(
            f"Cannot promote geometry type {geom.geom_type} to multi-polygon"
        )


def geom_alchemy(geom: Geom) -> str:
    geom = promote_to_multipolygon(geom)
    if geom.crs is None:
        raise ValueError("Geometry with no CRS")
    epsg = crs_to_epsg(geom.crs)
    return f"SRID={epsg};{geom.wkt}"


# 4326-like EPSG codes.
# extents in these CRSs will be projected to 4326, antimeridian-fixed, and then projected back, so it
# is very important that only CRSs where this is appropriate are included.
# It should only include CRSs that use the WGS84 datum, an equatorial cylindrical projection and are centred on
# the prime meridian (and therefore have a discontinuity at the anti-meridian).
#
# Just epsg:3857 for now (Web Mercator)
EPSG4326_LIKE_CODES = [3857]


def sanitise_extent(extent, crs) -> Geom:
    if crs.epsg == 4326:
        prelim = extent.to_crs(crs)
        return Geom(fix_shape(prelim.geom), crs=crs)
    elif crs.epsg in EPSG4326_LIKE_CODES:
        prelim = extent.to_crs("epsg:4326")
        fixed = Geom(fix_shape(prelim.geom), crs="epsg:4326")
        return fixed.to_crs(crs)
    else:
        return extent.to_crs(crs)


def generate_dataset_spatial_values(
    dataset_id, crs, extent: Geom | None
) -> dict[str, str] | None:
    extent = sanitise_extent(extent, crs)
    if extent is None:
        return None
    geom_alch = geom_alchemy(extent)
    return {"dataset_ref": dataset_id, "extent": geom_alch}


def extract_geometry_from_eo3_projection(eo3_gs_doc) -> Geom | None:
    native_crs = CRS(eo3_gs_doc["spatial_reference"])
    valid_data = eo3_gs_doc.get("valid_data")
    if valid_data:
        return Geom(valid_data, crs=native_crs)
    else:
        geo_ref_points = eo3_gs_doc.get("geo_ref_points")
        if geo_ref_points:
            return polygon(
                [
                    (geo_ref_points[key]["x"], geo_ref_points[key]["y"])
                    for key in ("ll", "ul", "ur", "lr", "ll")
                ],
                crs=native_crs,
            )
        else:
            return None
