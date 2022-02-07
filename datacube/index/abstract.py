# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime

from abc import ABC, abstractmethod
from typing import (Any, Callable, Iterable,
                    List, Mapping, Optional,
                    Tuple, Union)
from uuid import UUID


class AbstractUserResource(ABC):
    """
    Abstract base class for the User portion of an index api.

    All UserResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def grant_role(self, role: str, *usernames: str) -> None:
        """
        Grant a role to users
        :param role: name of the database role
        :param usernames: usernames to grant the role to.
        """

    @abstractmethod
    def create_user(self,
                    username: str,
                    password: str,
                    role: str,
                    description: Optional[str] = None) -> None:
        """
        Create a new user
        :param username: username of the new user
        :param password: password of the new user
        :param role: default role of the the new user
        :param description: optional description for the new user
        """

    @abstractmethod
    def delete_user(self,
                    *usernames: str
                   ) -> None:
        """
        Delete database users
        :param usernames: usernames of users to be deleted
        """

    @abstractmethod
    def list_users(self) -> Iterable[Tuple[str, str, str]]:
        """
        List all database users
        :return: Iterable of (role, username, description) tuples
        """

MetadataType = "datacube.model.MetadataType"

# TODO: Move to datacube.utils.changes (currently has no typehints)
MetadataChange = Tuple[Tuple, Any, Any]
MetadataAllowedChanges = Mapping[Tuple[str, ...], Callable[[str, str, Any, Any], bool]]

class AbstractMetadataTypeResource(ABC):
    """
    Abstract base class for the MetadataType portion of an index api.

    All MetadataTypeResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def from_doc(self, definition: Mapping[str, Any]) -> "datacube.model.AbstractMetadataType":
        """
        :param dict definition:
        :rtype: datacube.model.AbstractMetadataType
        """

    @abstractmethod
    def add(self,
            metadata_type: MetadataType,
            allow_table_lock: bool = False
           ) -> MetadataType:
        """
        Add a metadata type to the index.

        :param metadata_type: Unpersisted Metadatatype model
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.

            raise NotImplementedError if set to True, and this behaviour is not applicable
            for the implementing driver.
        :return: Persisted Metadatatype model.
        """

    @abstractmethod
    def can_update(self,
                   metadata_type: MetadataType,
                   allow_unsafe_updates: bool = False
                  ) -> Tuple[bool, Iterable[MetadataChange], Iterable[MetadataChange]]:
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param datacube.model.MetadataType metadata_type: updated MetadataType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: bool,list[change],list[change]
        """

    @abstractmethod
    def update(self,
               metadata_type: MetadataType,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False
              ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param datacube.model.MetadataType metadata_type: updated MetadataType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: datacube.model.MetadataType
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                       ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param dict definition: Updated definition
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: datacube.model.MetadataType
        """
        return self.update(self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates)

    def get(self, id_: int) -> Optional[MetadataType]:
        """
        Fetch metadata type by id.

        :rtype: datacube.model.MetadataType or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[MetadataType]:
        """
        Fetch metadata type by name.

        :rtype: datacube.model.MetadataType or None if not found
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @abstractmethod
    def get_unsafe(self, id_: int) -> MetadataType:
        """
        Fetch metadata type by id

        :param id_:
        :return: metadata type
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> MetadataType:
        """
        Fetch metadata type by name

        :param name:
        :return: metadata type
        :raises KeyError: if not found
        """

    @abstractmethod
    def check_field_indexes(self,
                            allow_table_lock: bool = False,
                            rebuild_views: bool = False,
                            rebuild_indexes: bool = False
                           ) -> None:
        """
        Create or replace per-field indexes and views.

        May have no effect if not relevant for this index implementation

        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """

    @abstractmethod
    def get_all(self) -> Iterable[MetadataType]:
        """
        Retrieve all Metadata Types

        :rtype: iter[datacube.model.MetadataType]
        """


Product = "datacube.model.DatasetType"

class AbstractProductResource(ABC):
    """
    Abstract base class for the Product portion of an index api.

    All ProductResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def from_doc(self, definition: Mapping[str, Any]) -> Product:
        """
        :param dict definition:
        :rtype: datacube.model.DatasetType
        """

    @abstractmethod
    def add(self,
            product: Product,
            allow_table_lock: bool = False
           ) -> Product:
        """
        Add a product to the index.

        :param metadata_type: Unpersisted Product model
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.

            raise NotImplementedError if set to True, and this behaviour is not applicable
            for the implementing driver.
        :return: Persisted Product model.
        """

    @abstractmethod
    def can_update(self,
                   product: Product,
                   allow_unsafe_updates: bool = False,
                   allow_table_lock: bool = False
                  ) -> Tuple[bool, Iterable[MetadataChange], Iterable[MetadataChange]]:
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param datacube.model.DatasetType product: product to update
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: bool,list[change],list[change]
        """

    @abstractmethod
    def update(self,
               metadata_type: Product,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False
               ) -> Product:
        """
        Update a product from the document. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param datacube.model.DatasetType metadata_type: updated DatasetType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: datacube.model.DatasetType
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                        allow_table_lock: bool = False
                       ) -> Product:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param dict definition: Updated definition
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: datacube.model.DatasetType
        """
        return self.update(self.from_doc(definition),
                           allow_unsafe_updates=allow_unsafe_updates,
                           allow_table_lock=allow_table_lock
                          )

    def add_document(self, definition: Mapping[str, Any]) -> Product:
        """
        Add a Product using its definition

        :param dict definition: product definition document
        :rtype: DatasetType
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    def get(self, id_: int) -> Optional[Product]:
        """
        Fetch product by id.

        :rtype: datacube.model.DatasetType or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[Product]:
        """
        Fetch product by name.

        :rtype: datacube.model.DatasetType or None if not found
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @abstractmethod
    def get_unsafe(self, id_: int) -> Product:
        """
        Fetch product by id

        :param id_:
        :return: product
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> Product:
        """
        Fetch product by name

        :param name:
        :return: product
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[Product]:
        """
        Return products that have all the given fields.

        :param iter[str] field_names:
        :rtype: __generator[DatasetType]
        """

    def search(self, **query: Any) -> Iterable[Product]:
        """
        Return products that have all the given fields.

        :param dict query:
        :rtype: __generator[DatasetType]
        """
        for type_, q in self.search_robust(**query):
            if not q:
                yield type_

    @abstractmethod
    def search_robust(self,
                      **query: Any
                     ) -> Iterable[Tuple[Product, Mapping]]:
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param dict query:
        :rtype: __generator[(DatasetType, dict)]
        """

    @abstractmethod
    def get_all(self) -> Iterable[Product]:
        """
        Retrieve all Products
        """


DSID = Union[str, UUID]
Dataset = "datacube.model.Dataset"


class AbstractDatasetResource(ABC):
    """
    Abstract base class for the Dataset portion of an index api.

    All DatasetResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def get(self,
            id_: DSID,
            include_sources: bool = False
           ) -> Optional[Dataset]:
        """
        Get dataset by id

        :param UUID or str id_: id of the dataset to retrieve
        :param bool include_sources: get the full provenance graph?
        :rtype: Dataset (None if not found)
        """

    @abstractmethod
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        """
        Get multiple datasets by id. (Lineage sources NOT included)

        :param Iterable of UUIDs and/or strings ids: ids to retrieve
        :return: Iterable of Datasets
        """

    @abstractmethod
    def get_derived(self, id_: UUID) -> Iterable[Dataset]:
        """
        Get all datasets derived from a dataset

        :param UUID id_: dataset id
        :rtype: list[Dataset]
        """

    @abstractmethod
    def has(self, id_: DSID) -> bool:
        """
        Is this dataset in this index?

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: bool
        """

    @abstractmethod
    def bulk_has(self, ids_: Iterable[DSID]) -> Iterable[bool]:
        """
        Like `has` but operates on a list of ids.

        For every supplied id check if database contains a dataset with that id.

        :param [typing.Union[UUID, str]] ids_: list of dataset ids

        :rtype: [bool]
        """

    @abstractmethod
    def add(self,
            dataset: Dataset,
            with_lineage: Optional[bool] = None
           ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: dataset to add

        :param with_lineage:
           - ``True|None`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

        :rtype: Dataset
        """

    @abstractmethod
    def search_product_duplicates(self,
                                  product: Product,
                                  *args: str
                                 ) -> Iterable[Tuple[Tuple, Iterable[UUID]]]:
        """
        Find dataset ids who have duplicates of the given set of field names.

        Product is always inserted as the first grouping field.

        Returns a generator returning a tuple containing a namedtuple of
        the values of the supplied fields, and the datasets that match those
        values.

        :param product: The Product to restrict search to
        :param args: field names to identify duplicates over
        """

    @abstractmethod
    def can_update(self,
                   dataset: Dataset,
                   updates_allowed: Optional[MetadataAllowedChanges] = None
                  ):
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param Dataset dataset: Dataset to update
        :param dict updates_allowed: Allowed updates
        :rtype: bool,list[change],list[change]
        """

    @abstractmethod
    def update(self,
               dataset: Dataset,
               updates_allowed: Optional[MetadataAllowedChanges] = None
              ) -> Dataset:
        """
        Update dataset metadata and location
        :param Dataset dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :rtype: Dataset
        """

    @abstractmethod
    def archive(self, ids: Iterable[UUID]) -> None:
        """
        Mark datasets as archived

        :param Iterable[UUID] ids: list of dataset ids to archive
        """

    @abstractmethod
    def restore(self, ids: Iterable[UUID]) -> None:
        """
        Mark datasets as not archived

        :param Iterable[UUID] ids: list of dataset ids to restore
        """

    @abstractmethod
    def purge(self, ids: Iterable[UUID]) -> None:
        """
        Delete archived datasets

        :param ids: iterable of dataset ids to purge
        """

    @abstractmethod
    def get_all_dataset_ids(self, archived: bool) -> Iterable[str]:
        """
        Get all dataset IDs based only on archived status

        This will be very slow and inefficient for large databases, and is really
        only intended for small and/or experimental databases.

        :param archived:
        :rtype: list[str]
        """

    @abstractmethod
    def get_field_names(self, product_name: Optional[str] = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param Optional[str] product_name: None for all products
        :rtype: set[str]
        """

    @abstractmethod
    def get_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get the list of storage locations for the given dataset id

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: list[str]
        """

    @abstractmethod
    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        """
        Find locations which have been archived for a dataset

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: list[str]
        """

    @abstractmethod
    def get_archived_location_times(self,
                                    id_: DSID
                                   ) -> Iterable[Tuple[str, datetime.datetime]]:
        """
        Get each archived location along with the time it was archived.

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: List[Tuple[str, datetime.datetime]]
        """

    @abstractmethod
    def add_location(self, id_: DSID, uri: str) -> bool:
        """
        Add a location to the dataset if it doesn't already exist.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :returns bool: Was one added?
        """

    @abstractmethod
    def get_datasets_for_location(self,
                                  uri: str,
                                  mode: Optional[str] = None
                                 ) -> Iterable[Dataset]:
        """
        Find datasets that exist at the given URI

        :param uri: search uri
        :param str mode: 'exact', 'prefix' or None (to guess)
        :return:
        """

    @abstractmethod
    def remove_location(self,
                        id_: DSID,
                        uri: str
                       ) -> bool:
        """
        Remove a location from the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :returns bool: Was one removed?
        """

    @abstractmethod
    def archive_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Archive a location of the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :return bool: location was able to be archived
        """

    @abstractmethod
    def restore_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Un-archive a location of the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :return bool: location was able to be restored
        """

    @abstractmethod
    def search_by_metadata(self,
                           metadata: Mapping[str, Any]
                          ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution – slow! This will usually not use indexes.

        :param dict metadata:
        :rtype: list[Dataset]
        """

    @abstractmethod
    def search(self,
               limit: Optional[int] = None,
               **query: Any) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param Union[str,float,Range,list] query:
        :param Optional[int] limit: Limit number of dataset (None = unlimited)
        :rtype: __generator[Dataset]
        """

    @abstractmethod
    def search_by_product(self,
                          **query: Any
                         ) -> Iterable[Tuple[Iterable[Dataset], Product]]:
        """
        Perform a search, returning datasets grouped by product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[(DatasetType,  __generator[Dataset])]]
        """

    @abstractmethod
    def search_returning(self,
                         field_names: Iterable[str],
                         limit: Optional[int] = None,
                         **query: Any
                        ) -> Iterable[Tuple]:
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param tuple[str] field_names:
        :param Union[str,float,Range,list] query:
        :param int limit: Limit number of datasets
        :returns __generator[tuple]: sequence of results, each result is a namedtuple of your requested fields
        """

    @abstractmethod
    def count(self, **query: Any) -> int:
        """
        Perform a search, returning count of results.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: int
        """

    @abstractmethod
    def count_by_product(self, **query: Any) -> Iterable[Tuple[Product, int]]:
        """
        Perform a search, returning a count of for each matching product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :returns: Sequence of (product, count)
        :rtype: __generator[(DatasetType,  int)]]
        """

    @abstractmethod
    def count_by_product_through_time(self,
                                      period: str,
                                      **query: Any
                                     ) -> Iterable[Tuple[Product, Iterable[Tuple[datetime.datetime, datetime.datetime]], int]]:
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param dict[str,str|float|datacube.model.Range] query:
        :param str period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        :rtype: __generator[(DatasetType, list[(datetime.datetime, datetime.datetime), int)]]
        """

    @abstractmethod
    def count_product_through_time(self,
                                   period: str,
                                   **query: Any
                                  ) -> Iterable[Tuple[str, Iterable[Tuple[datetime.datetime, datetime.datetime]], int]]:
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param dict[str,str|float|datacube.model.Range] query:
        :param str period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        :rtype: list[(str, list[(datetime.datetime, datetime.datetime), int)]]
        """

    @abstractmethod
    def search_summaries(self, **query: Any) -> Iterable[Mapping[str, Any]]:
        """
        Perform a search, returning just the search fields of each dataset.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[dict]
        """

    def search_eager(self, **query: Any) -> List[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: list[Dataset]
        """
        return list(self.search(**query))

    @abstractmethod
    def get_product_time_bounds(self,
                                product: str
                               ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.
        """

    @abstractmethod
    def search_returning_datasets_light(self,
                                        field_names: Tuple[str, ...],
                                        custom_offsets: Optional[Mapping[str, str]] = None,
                                        limit: Optional[int] = None,
                                        **query: Any
                                       ) -> Iterable[Tuple]:
        """
        This is a dataset search function that returns the results as objects of a dynamically
        generated Dataset class that is a subclass of tuple.

        Only the requested fields will be returned together with related derived attributes as property functions
        similer to the datacube.model.Dataset class. For example, if 'extent'is requested all of
        'crs', 'extent', 'transform', and 'bounds' are available as property functions.

        The field_names can be custom fields in addition to those specified in metadata_type, fixed fields, or
        native fields. The field_names can also be derived fields like 'extent', 'crs', 'transform',
        and 'bounds'. The custom fields require custom offsets of the metadata doc be provided.

        The datasets can be selected based on values of custom fields as long as relevant custom
        offsets are provided. However custom field values are not transformed so must match what is
        stored in the database.

        :param field_names: A tuple of field names that would be returned including derived fields
                            such as extent, crs
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields
        :param limit: Number of datasets returned per product.
        :param query: key, value mappings of query that will be processed against metadata_types,
                      product definitions and/or dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of namedtuple and possibly with
        property functions).
        """


class AbstractIndex(ABC):
    """
    Abstract base class for an Index.  All Index implementations should
    inherit from this base class and implement all abstract methods.
    """

    @property
    @abstractmethod
    def url(self) -> str: pass

    @property
    @abstractmethod
    def users(self) -> AbstractUserResource: pass

    @property
    @abstractmethod
    def metadata_types(self) -> AbstractMetadataTypeResource: pass

    @property
    @abstractmethod
    def products(self) -> AbstractProductResource: pass

    @property
    @abstractmethod
    def datasets(self) -> AbstractDatasetResource: pass

    @classmethod
    @abstractmethod
    def from_config(cls,
                    config: "datacube.config.LocalConfig",
                    application_name: Optional[str] = None,
                    validate_connection: bool = True
                   ) -> "AbstractIndex":
        pass

    @classmethod
    @abstractmethod
    def get_dataset_fields(cls,
                           doc: dict
                          ) -> Mapping[str, "datacube.model.fields.Field"]:
        pass

    @abstractmethod
    def init_db(self,
                with_default_types: bool = True,
                with_permissions: bool = True) -> bool: pass

    @abstractmethod
    def close(self) -> None: pass

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class AbstractIndexDriver(ABC):
    """
    Abstract base class for an IndexDriver.  All IndexDrivers should inherit from this base class
    and implement all abstract methods.
    """
    @staticmethod
    @abstractmethod
    def connect_to_index(config: "datacube.config.LocalConfig",
                         application_name: Optional[str] = None,
                         validate_connection: bool = True
                        ) -> "datacube.index.AbstractIndex":
        pass

    @staticmethod
    @abstractmethod
    def metadata_type_from_doc(
                               definition: dict
                              ) -> MetadataType:
        pass
