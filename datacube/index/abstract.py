# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
from pathlib import Path

from abc import ABC, abstractmethod
from typing import (Any, Iterable, Iterator,
                    List, Mapping, Optional,
                    Tuple, Union)
from uuid import UUID

from datacube.config import LocalConfig
from datacube.index.fields import Field
from datacube.model import Dataset, MetadataType, Range
from datacube.model import DatasetType as Product
from datacube.utils import cached_property, read_documents, InvalidDocException
from datacube.utils.changes import AllowPolicy, Change, Offset


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
    def list_users(self) -> Iterable[Tuple[str, str, Optional[str]]]:
        """
        List all database users
        :return: Iterable of (role, username, description) tuples
        """


_DEFAULT_METADATA_TYPES_PATH = Path(__file__).parent.joinpath('default-metadata-types.yaml')


def default_metadata_type_docs() -> List[MetadataType]:
    """A list of the bare dictionary format of default :class:`datacube.model.MetadataType`"""
    return [doc for (path, doc) in read_documents(_DEFAULT_METADATA_TYPES_PATH)]


class AbstractMetadataTypeResource(ABC):
    """
    Abstract base class for the MetadataType portion of an index api.

    All MetadataTypeResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def from_doc(self, definition: Mapping[str, Any]) -> MetadataType:
        """
        Construct a MetadataType object from a dictionary definition

        :param definition: A metadata definition dictionary
        :return: An unpersisted MetadataType object
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
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: updated MetadataType
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
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

        :param metadata_type: MetadataType model with unpersisted updates
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated MetadataType model
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                       ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param definition: Updated definition
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Persisted updated MetadataType model
        """
        return self.update(self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates)

    def get(self, id_: int) -> Optional[MetadataType]:
        """
        Fetch metadata type by id.

        :return: MetadataType model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[MetadataType]:
        """
        Fetch metadata type by name.

        :return: MetadataType model or None if not found
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
        :return: metadata type model
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> MetadataType:
        """
        Fetch metadata type by name

        :param name:
        :return: metadata type model
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
        :param: rebuild_views: whether or not views should be rebuilt
        :param: rebuild_indexes: whether or not views should be rebuilt
        """

    @abstractmethod
    def get_all(self) -> Iterable[MetadataType]:
        """
        Retrieve all Metadata Types

        :returns: All available MetadataType models
        """


QueryField = Union[str, float, int, Range, datetime.datetime]
QueryDict = Mapping[str, QueryField]


class AbstractProductResource(ABC):
    """
    Abstract base class for the Product portion of an index api.

    All ProductResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """
    metadata_type_resource: AbstractMetadataTypeResource

    def from_doc(self, definition: Mapping[str, Any]) -> Product:
        """
        Construct unpersisted Product model from product metadata dictionary

        :param definition: a Product metadata dictionary
        :return: Unpersisted product model
        """
        # This column duplication is getting out of hand:
        Product.validate(definition)   # type: ignore[attr-defined]   # validate method added by decorator
        # Validate extra dimension metadata
        Product.validate_extra_dims(definition)

        metadata_type = definition['metadata_type']

        # They either specified the name of a metadata type, or specified a metadata type.
        # Is it a name?
        if isinstance(metadata_type, str):
            metadata_type = self.metadata_type_resource.get_by_name(metadata_type)
        else:
            # Otherwise they embedded a document, add it if needed:
            metadata_type = self.metadata_type_resource.from_doc(metadata_type)
            definition = dict(definition)
            definition['metadata_type'] = metadata_type.name

        if not metadata_type:
            raise InvalidDocException('Unknown metadata type: %r' % definition['metadata_type'])

        return Product(metadata_type, definition)

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
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param product: product to update
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(self,
               metadata_type: Product,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False
               ) -> Product:
        """
        Persist updates to a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param metadata_type: Product model with unpersisted updates
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated Product model
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                        allow_table_lock: bool = False
                       ) -> Product:
        """
        Update a metadata type from a document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param definition: Updated definition
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated Product model
        """
        return self.update(self.from_doc(definition),
                           allow_unsafe_updates=allow_unsafe_updates,
                           allow_table_lock=allow_table_lock
                          )

    def add_document(self, definition: Mapping[str, Any]) -> Product:
        """
        Add a Product using its definition

        :param dict definition: product definition document
        :return: Persisted Product model
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    def get(self, id_: int) -> Optional[Product]:
        """
        Fetch product by id.

        :param id_: Id of desired product
        :return: Product model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[Product]:
        """
        Fetch product by name.

        :param name: Name of desired product
        :return: Product model or None if not found
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @abstractmethod
    def get_unsafe(self, id_: int) -> Product:
        """
        Fetch product by id

        :param id_: id of desired product
        :return: product model
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> Product:
        """
        Fetch product by name

        :param name: name of desired product
        :return: product model
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[Product]:
        """
        Return products that have all of the given fields.

        :param field_names: names of fields that returned products must have
        :returns: Matching product models
        """

    def search(self, **query: QueryField) -> Iterator[Product]:
        """
        Return products that match the supplied query

        :param query: Query parameters
        :return: Generator of product models
        """
        for type_, q in self.search_robust(**query):
            if not q:
                yield type_

    @abstractmethod
    def search_robust(self,
                      **query: QueryField
                     ) -> Iterable[Tuple[Product, Mapping[str, QueryField]]]:
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param query: Query parameters
        :return: Tuples of product model and a dict of remaining unmatchable fields
        """

    @abstractmethod
    def get_all(self) -> Iterable[Product]:
        """
        Retrieve all Products

        :returns: Product models for all known products
        """


# Non-strict Dataset ID representation
DSID = Union[str, UUID]


def dsid_to_uuid(dsid: DSID) -> UUID:
    if isinstance(dsid, UUID):
        return dsid
    else:
        return UUID(dsid)


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

        :param id_: id of the dataset to retrieve
        :param include_sources: get the full provenance graph?
        :rtype: Dataset model (None if not found)
        """

    @abstractmethod
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        """
        Get multiple datasets by id. (Lineage sources NOT included)

        :param ids: ids to retrieve
        :return: Iterable of Dataset models
        """

    @abstractmethod
    def get_derived(self, id_: DSID) -> Iterable[Dataset]:
        """
        Get all datasets derived from a dataset (NOT recursive)

        :param id_: dataset id
        :rtype: list[Dataset]
        """

    @abstractmethod
    def has(self, id_: DSID) -> bool:
        """
        Is this dataset in this index?

        :param id_: dataset id
        :return: True if the dataset exists in this index
        """

    @abstractmethod
    def bulk_has(self, ids_: Iterable[DSID]) -> Iterable[bool]:
        """
        Like `has` but operates on a multiple ids.

        For every supplied id check if database contains a dataset with that id.

        :param ids_: iterable of dataset ids to check existence in index

        :return: Iterable of bools, true for datasets that exist in index
        """

    @abstractmethod
    def add(self, dataset: Dataset,
            with_lineage: bool = True
           ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: Unpersisted dataset model

        :param with_lineage:
           - ``True (default)`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

        :return: Persisted Dataset model
        """

    @abstractmethod
    def search_product_duplicates(self,
                                  product: Product,
                                  *args: Union[str, Field]
                                 ) -> Iterable[Tuple[Tuple, Iterable[UUID]]]:
        """
        Find dataset ids who have duplicates of the given set of field names.

        (Search is always restricted by Product)

        Returns a generator returning a tuple containing a namedtuple of
        the values of the supplied fields, and the datasets that match those
        values.

        :param product: The Product to restrict search to
        :param args: field names to identify duplicates over
        """

    @abstractmethod
    def can_update(self,
                   dataset: Dataset,
                   updates_allowed: Optional[Mapping[Offset, AllowPolicy]] = None
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param Dataset dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(self,
               dataset: Dataset,
               updates_allowed: Optional[Mapping[Offset, AllowPolicy]] = None
              ) -> Dataset:
        """
        Update dataset metadata and location
        :param Dataset dataset: Dataset model with unpersisted updates
        :param updates_allowed: Allowed updates
        :return: Persisted dataset model
        """

    @abstractmethod
    def archive(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as archived

        :param Iterable[Union[str,UUID]] ids: list of dataset ids to archive
        """

    @abstractmethod
    def restore(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as not archived

        :param Iterable[Union[str,UUID]] ids: list of dataset ids to restore
        """

    @abstractmethod
    def purge(self, ids: Iterable[DSID]) -> None:
        """
        Delete archived datasets

        :param ids: iterable of dataset ids to purge
        """

    @abstractmethod
    def get_all_dataset_ids(self, archived: bool) -> Iterable[UUID]:
        """
        Get all dataset IDs based only on archived status

        This will be very slow and inefficient for large databases, and is really
        only intended for small and/or experimental databases.

        :param archived: If true, return all archived datasets, if false, all unarchived datatsets
        :return: Iterable of dataset ids
        """

    @abstractmethod
    def get_field_names(self, product_name: Optional[str] = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param product_name: Name of product, or None for all products
        :return: All possible search field names
        """

    @abstractmethod
    def get_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get (active) storage locations for the given dataset id

        :param id_: dataset id
        :return: Storage locations for the dataset
        """

    @abstractmethod
    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get archived locations for a dataset

        :param id_: dataset id
        :return: Archived storage locations for the dataset
        """

    @abstractmethod
    def get_archived_location_times(self,
                                    id_: DSID
                                   ) -> Iterable[Tuple[str, datetime.datetime]]:
        """
        Get each archived location along with the time it was archived.

        :param id_: dataset id
        :return: Archived storage locations, with archive date.
        """

    @abstractmethod
    def add_location(self, id_: DSID, uri: str) -> bool:
        """
        Add a location to the dataset if it doesn't already exist.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if a location was added, false if location already existed
        """

    @abstractmethod
    def get_datasets_for_location(self,
                                  uri: str,
                                  mode: Optional[str] = None
                                 ) -> Iterable[Dataset]:
        """
        Find datasets that exist at the given URI

        :param uri: search uri
        :param mode: 'exact', 'prefix' or None (to guess)
        :return: Matching dataset models
        """

    @abstractmethod
    def remove_location(self,
                        id_: DSID,
                        uri: str
                       ) -> bool:
        """
        Remove a location from the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was removed, false if it didn't exist for the database
        """

    @abstractmethod
    def archive_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Archive a location of the dataset if it exists and is active.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was able to be archived
        """

    @abstractmethod
    def restore_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Un-archive a location of the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True location was able to be restored
        """

    @abstractmethod
    def search_by_metadata(self,
                           metadata: Mapping[str, QueryField]
                          ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution – slow! This will usually not use indexes.

        :param metadata: metadata dictionary representing arbitrary search query
        :return: Matching dataset models
        """

    @abstractmethod
    def search(self,
               limit: Optional[int] = None,
               source_filter: Optional[Mapping[str, QueryField]] = None,
               **query: QueryField) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param limit: Limit number of dataset (None/default = unlimited)
        :param query: search query parameters
        :return: Matching datasets
        """

    @abstractmethod
    def search_by_product(self,
                          **query: QueryField
                         ) -> Iterable[Tuple[Iterable[Dataset], Product]]:
        """
        Perform a search, returning datasets grouped by product type.

        :param query: search query parameters
        :return: Matching datasets, grouped by Product
        """

    @abstractmethod
    def search_returning(self,
                         field_names: Iterable[str],
                         limit: Optional[int] = None,
                         **query: QueryField
                        ) -> Iterable[Tuple]:
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param field_names: Names of desired fields
        :param limit: Limit number of dataset (None/default = unlimited)
        :param query: search query parameters
        :return: Namedtuple of requested fields, for each matching dataset.
        """

    @abstractmethod
    def count(self, **query: QueryField) -> int:
        """
        Perform a search, returning count of results.

        :param query: search query parameters
        :return: Count of matching datasets in index
        """

    @abstractmethod
    def count_by_product(self, **query: QueryField) -> Iterable[Tuple[Product, int]]:
        """
        Perform a search, returning a count of for each matching product type.

        :param query: search query parameters
        :return: Counts of matching datasets in index, grouped by product.
        """

    @abstractmethod
    def count_by_product_through_time(self,
                                      period: str,
                                      **query: QueryField
                                     ) -> Iterable[Tuple[Product, Iterable[Tuple[Range, int]]]]:
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param query: search query parameters
        :returns: For each matching product type, a list of time ranges and their count.
        """

    @abstractmethod
    def count_product_through_time(self,
                                   period: str,
                                   **query: QueryField
                                  ) -> Iterable[Tuple[Range, int]]:
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param query: search query parameters
        :returns: The product, a list of time ranges and the count of matching datasets.
        """

    @abstractmethod
    def search_summaries(self, **query: QueryField) -> Iterable[Mapping[str, Any]]:
        """
        Perform a search, returning just the search fields of each dataset.

        :param query: search query parameters
        :return: Mappings of search fields for matching datasets
        """

    def search_eager(self, **query: QueryField) -> List[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param query: search query parameters
        :return: Fully instantiated list of matching dataset models
        """
        return list(self.search(**query))  # type: ignore[arg-type]   # mypy isn't being very smart here :(

    @abstractmethod
    def get_product_time_bounds(self,
                                product: str
                               ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.

        :param product: Name of product
        :return: minimum and maximum acquisition times
        """

    @abstractmethod
    def search_returning_datasets_light(self,
                                        field_names: Tuple[str, ...],
                                        custom_offsets: Optional[Mapping[str, Offset]] = None,
                                        limit: Optional[int] = None,
                                        **query: QueryField
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
        :param query: query parameters that will be processed against metadata_types,
                      product definitions and/or dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of namedtuple and possibly with
        property functions).
        """


class AbstractIndex(ABC):
    """
    Abstract base class for an Index.  All Index implementations should
    inherit from this base class and implement all abstract methods.
    """

    # Interface contracts
    #   supports add() update() remove() etc methods.
    supports_persistance = True
    #   supports legacy ODCv1 EO style metadata types.
    supports_legacy = True
    #   supports non-geospatial (e.g. telemetry) metadata types
    supports_nongeo = True

    @property
    @abstractmethod
    def url(self) -> str: ...

    @property
    @abstractmethod
    def users(self) -> AbstractUserResource: ...

    @property
    @abstractmethod
    def metadata_types(self) -> AbstractMetadataTypeResource: ...

    @property
    @abstractmethod
    def products(self) -> AbstractProductResource: ...

    @property
    @abstractmethod
    def datasets(self) -> AbstractDatasetResource: ...

    @classmethod
    @abstractmethod
    def from_config(cls,
                    config: LocalConfig,
                    application_name: Optional[str] = None,
                    validate_connection: bool = True
                   ) -> "AbstractIndex":
        ...

    @classmethod
    @abstractmethod
    def get_dataset_fields(cls,
                           doc: dict
                          ) -> Mapping[str, Field]:
        ...

    @abstractmethod
    def init_db(self,
                with_default_types: bool = True,
                with_permissions: bool = True) -> bool:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

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
    def connect_to_index(config: LocalConfig,
                         application_name: Optional[str] = None,
                         validate_connection: bool = True
                        ) -> "AbstractIndex":
        pass

    @staticmethod
    @abstractmethod
    def metadata_type_from_doc(definition: dict
                              ) -> MetadataType:
        pass


# The special handling of grid_spatial, etc appears to NOT apply to EO3.
# Does EO3 handle it in metadata?
class DatasetSpatialMixin:
    __slots__ = ()

    @property
    def _gs(self):
        return self.grid_spatial

    @property
    def crs(self):
        return Dataset.crs.__get__(self)

    @cached_property
    def extent(self):
        return Dataset.extent.func(self)

    @property
    def transform(self):
        return Dataset.transform.__get__(self)

    @property
    def bounds(self):
        return Dataset.bounds.__get__(self)
