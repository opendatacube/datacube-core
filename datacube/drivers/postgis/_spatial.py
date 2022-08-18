# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Tracking spatial indexes
"""

import logging
from threading import Lock
from typing import Mapping, Optional, Type, Union

from sqlalchemy import ForeignKey, select
from sqlalchemy.dialects import postgresql as postgres
from geoalchemy2 import Geometry

from sqlalchemy.engine import Connectable
from sqlalchemy import Column
from sqlalchemy.orm import Session

from datacube.utils.geometry import CRS
from ._core import METADATA
from .sql import SCHEMA_NAME
from ._schema import orm_registry, Dataset, SpatialIndex, SpatialIndexRecord

_LOG = logging.getLogger(__name__)


# In theory we could just use the SQLAlchemy registry for this, but it is not indexed
# in a useful way.
class SpatialIndexORMRegistry:
    """Threadsafe global registry of SpatialIndex ORM classes, indexed by EPSG/SRID code."""
    _registry: Mapping[int, Type[SpatialIndex]] = {}
    _lock = Lock()

    def __init__(self):
        self._registry = self.__class__._registry
        self._lock = self.__class__._lock

    def _to_epsg(self, epsg_or_crs: Union[CRS, int]) -> int:
        """Utility method to convert a epsg_or_crs to an epsg."""
        if isinstance(epsg_or_crs, CRS):
            return epsg_or_crs.epsg
        else:
            return epsg_or_crs

    def register(self, epsg_or_crs: Union[CRS, int]) -> bool:
        """Ensure that SpatialIndex ORM clss is registered for this EPSG/SRID"""
        epsg = self._to_epsg(epsg_or_crs)
        added = False
        with self._lock:
            if epsg not in self._registry:
                self._registry[epsg] = self._mint_new_spindex(epsg)
                added = True
        return added

    def get(self, epsg_or_crs: Union[CRS, int]) -> Optional[Type[SpatialIndex]]:
        """Retrieve the registered SpatialIndex ORM class"""
        epsg = self._to_epsg(epsg_or_crs)
        return self._registry.get(epsg)

    def _mint_new_spindex(self, epsg: int):
        """
        Dynamically create a new ORM class for a EPSG/SRID.

        Note: Called within registry lock.
        """
        table_name = f"spatial_{epsg}"
        attributes = {
            '__tablename__': table_name,
            '__table_args__': (
                METADATA,
                {
                    "schema": SCHEMA_NAME,
                    "comment": "A product or dataset type, family of related datasets."
                }
            ),
            "dataset_ref": Column(postgres.UUID(as_uuid=True), ForeignKey(Dataset.id),
                                  primary_key=True,
                                  nullable=False,
                                  comment="The dataset being indexed")
        }
        # Add geometry column
        attributes["extent"] = Column(Geometry('MULTIPOLYGON', srid=epsg),
                                      nullable=False,
                                      comment="The extent of the dataset")
        return orm_registry.mapped(type(f'SpatialIdx{epsg}', (SpatialIndex,), attributes))


def spindex_for_epsg(epsg: int) -> Type[SpatialIndex]:
    """Return ORM class of a SpatialIndex for EPSG/SRID - dynamically creating if necessary"""
    sir = SpatialIndexORMRegistry()
    spindex = sir.get(epsg)
    if spindex is None:
        sir.register(epsg)
        spindex = sir.get(epsg)
    return spindex


def spindex_for_crs(crs: CRS) -> Type[SpatialIndex]:
    """Return ORM class of a SpatialIndex for CRS - dynamically creating if necessary"""
    if not (str(crs).startswith('EPSG') and crs.epsg):
        # Postgis identifies CRSs by a numeric "SRID" which is equivalent to EPSG number.
        _LOG.error("Cannot create a postgis spatial index for a non-EPSG-style CRS.")
        return None

    return spindex_for_epsg(crs.epsg)


def spindex_for_record(rec: SpatialIndexRecord) -> Type[SpatialIndex]:
    """Convert a Record of a SpatialIndex created in a particular database to an ORM class"""
    return spindex_for_crs(rec.crs)


def ensure_spindex(engine: Connectable, sp_idx: Type[SpatialIndex]) -> None:
    """Ensure a Spatial Index exists in a particular database."""
    with Session(engine) as session:
        results = session.execute(
            select(SpatialIndexRecord.srid).where(SpatialIndexRecord.srid == sp_idx.__tablename__[8:])
        )
        for result in results:
            # SpatialIndexRecord exists - actual index assumed to exist too.
            return
        # SpatialIndexRecord doesn't exist - create the index table...
        orm_registry.metadata.create_all(engine, [sp_idx.__table__])
        # ... and add a SpatialIndexRecord
        session.add(SpatialIndexRecord.from_spindex(sp_idx))
        session.flush()
    return


def spindexes(engine: Connectable) -> Mapping[CRS, Type[SpatialIndex]]:
    """
    Return a CRS-to-Spatial Index ORM class mapping for indexes that exist in a particular database.
    """
    out = {}
    sir = SpatialIndexORMRegistry()
    with Session(engine) as session:
        results = session.execute(select(SpatialIndexRecord.srid))
        for result in results:
            epsg = int(result[0])
            spindex = spindex_for_epsg(epsg)
            crs = CRS(f'EPSG:{epsg}')
            out[crs] = spindex
    return out
