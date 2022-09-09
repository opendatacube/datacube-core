# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any, Iterable, Sequence

from datacube.drivers.postgis import PostGisDb
from datacube.index.postgis._datasets import DatasetResource, DSID  # type: ignore
from datacube.index.postgis._metadata_types import MetadataTypeResource
from datacube.index.postgis._products import ProductResource
from datacube.index.postgis._users import UserResource
from datacube.index.abstract import AbstractIndex, AbstractIndexDriver, default_metadata_type_docs, AbstractTransaction
from datacube.model import MetadataType
from datacube.utils.geometry import CRS

_LOG = logging.getLogger(__name__)


class PostgisTransaction(AbstractTransaction):
    def __init__(self, db: PostGisDb, idx_id: str) -> None:
        super().__init__(idx_id)
        self._db = db

    def _new_connection(self) -> Any:
        return self._db.begin()

    def _commit(self) -> None:
        self._connection.commit()

    def _rollback(self) -> None:
        self._connection.rollback()

    def _release_connection(self) -> None:
        self._connection.close()


class Index(AbstractIndex):
    """
    Access to the datacube index.

    DON'T INITIALISE THIS DIRECTLY (it will break in the future). Use `datacube.index.index_connect()` or
    access property ``.index`` on your existing :class:`datacube.api.core.Datacube`.

    These are thread safe. But not multiprocess safe once a connection is made (db connections cannot be shared
    between processes) You can close idle connections before forking by calling close(), provided you know no
    other connections are active. Or else use a separate instance of this class in each process.

    :ivar datacube.index._datasets.DatasetResource datasets: store and retrieve :class:`datacube.model.Dataset`
    :ivar datacube.index._products.ProductResource products: store and retrieve :class:`datacube.model.Product`\
    (formerly called DatasetType)
    :ivar datacube.index._metadata_types.MetadataTypeResource metadata_types: store and retrieve \
    :class:`datacube.model.MetadataType`
    :ivar UserResource users: user management

    :type users: datacube.index._users.UserResource
    :type datasets: datacube.index._datasets.DatasetResource
    :type products: datacube.index._products.ProductResource
    :type metadata_types: datacube.index._metadata_types.MetadataTypeResource
    """

    # Postgis driver does not need to support pre-EO3 metadata formats
    supports_legacy = False
    # Hopefully can reinstate non-geo support, but dropping for now will make progress easier.
    supports_nongeo = False
    # Hopefully can reinstate a simpler form of lineage support, but leave for now
    supports_lineage = False
    supports_source_filters = False
    supports_transactions = True

    def __init__(self, db: PostGisDb) -> None:
        # POSTGIS driver is not stable with respect to database schema or internal APIs.
        _LOG.warning("""WARNING: The POSTGIS index driver implementation is considered EXPERIMENTAL.
WARNING:
WARNING: Database schema and internal APIs may change significantly between releases. Use at your own risk.""")
        self._db = db

        self._users = UserResource(db)
        self._metadata_types = MetadataTypeResource(db)
        self._products = ProductResource(db, self.metadata_types)
        self._datasets = DatasetResource(db, self.products)

    @property
    def users(self) -> UserResource:
        return self._users

    @property
    def metadata_types(self) -> MetadataTypeResource:
        return self._metadata_types

    @property
    def products(self) -> ProductResource:
        return self._products

    @property
    def datasets(self) -> DatasetResource:
        return self._datasets

    @property
    def url(self) -> str:
        return str(self._db.url)

    @classmethod
    def from_config(cls, config, application_name=None, validate_connection=True):
        db = PostGisDb.from_config(config, application_name=application_name,
                                   validate_connection=validate_connection)
        return cls(db)

    @classmethod
    def get_dataset_fields(cls, doc):
        return PostGisDb.get_dataset_fields(doc)

    def init_db(self, with_default_types=True, with_permissions=True, with_default_spatial_index=True):
        is_new = self._db.init(with_permissions=with_permissions)

        if is_new and with_default_types:
            _LOG.info('Adding default metadata types.')
            for doc in default_metadata_type_docs():
                self.metadata_types.add(self.metadata_types.from_doc(doc), allow_table_lock=True)

        if is_new and with_default_spatial_index:
            self.create_spatial_index(CRS("EPSG:4326"))

        return is_new

    def close(self):
        """
        Close any idle connections database connections.

        This is good practice if you are keeping the Index instance in scope
        but wont be using it for a while.

        (Connections are normally closed automatically when this object is deleted: ie. no references exist)
        """
        self._db.close()

    @property
    def index_id(self) -> str:
        return self.url

    def transaction(self) -> AbstractTransaction:
        return PostgisTransaction(self._db, self.index_id)

    def create_spatial_index(self, crs: CRS) -> bool:
        sp_idx = self._db.create_spatial_index(crs)
        return sp_idx is not None

    def spatial_indexes(self, refresh=False) -> Iterable[CRS]:
        return self._db.spatial_indexes(refresh)

    def update_spatial_index(self,
                             crses: Sequence[CRS] = [],
                             product_names: Sequence[str] = [],
                             dataset_ids: Sequence[DSID] = []
                             ) -> int:
        with self._db.connect() as conn:
            return conn.update_spindex(crses, product_names, dataset_ids)

    def __repr__(self):
        return "Index<db={!r}>".format(self._db)


class DefaultIndexDriver(AbstractIndexDriver):
    @staticmethod
    def connect_to_index(config, application_name=None, validate_connection=True):
        return Index.from_config(config, application_name, validate_connection)

    @staticmethod
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        # TODO: Validate metadata is ODCv2 compliant - e.g. either non-raster or EO3.
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(definition,
                            dataset_search_fields=Index.get_dataset_fields(definition))


def index_driver_init():
    return DefaultIndexDriver()
