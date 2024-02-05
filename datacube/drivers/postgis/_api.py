# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods,too-many-lines

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Persistence API implementation for postgis.
"""

import datetime
import json
import logging
import uuid  # noqa: F401
from sqlalchemy import cast
from sqlalchemy import delete, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.expression import Select
from sqlalchemy import select, text, and_, or_, func
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.exc import IntegrityError
from typing import Iterable, Sequence, Optional, Set

from datacube.index.fields import OrExpression
from datacube.model import Range
from odc.geo import CRS, Geometry
from datacube.utils.uris import split_uri
from datacube.index.abstract import DSID
from datacube.model.lineage import LineageRelation, LineageDirection
from . import _core
from ._fields import parse_fields, Expression, PgField, PgExpression  # noqa: F401
from ._fields import NativeField, DateDocField, SimpleDocField, UnindexableValue
from ._schema import MetadataType, Product, \
    Dataset, DatasetLineage, DatasetLocation, SelectedDatasetLocation, \
    search_field_index_map, search_field_indexes, DatasetHome
from ._spatial import geom_alchemy, generate_dataset_spatial_values, extract_geometry_from_eo3_projection
from .sql import escape_pg_identifier


_LOG = logging.getLogger(__name__)


# Make a function because it's broken
def _dataset_select_fields():
    return (
        Dataset,
        # All active URIs, from newest to oldest
        func.array(
            select(
                SelectedDatasetLocation.uri
            ).where(
                and_(
                    SelectedDatasetLocation.dataset_ref == Dataset.id,
                    SelectedDatasetLocation.archived == None
                )
            ).order_by(
                SelectedDatasetLocation.added.desc(),
                SelectedDatasetLocation.id.desc()
            ).label('uris')
        ).label('uris')
    )


def _dataset_bulk_select_fields():
    return (
        Dataset.product_ref,
        Dataset.metadata_doc,
        # All active URIs, from newest to oldest
        func.array(
            select(
                SelectedDatasetLocation.uri
            ).where(
                and_(
                    SelectedDatasetLocation.dataset_ref == Dataset.id,
                    SelectedDatasetLocation.archived == None
                )
            ).order_by(
                SelectedDatasetLocation.added.desc(),
                SelectedDatasetLocation.id.desc()
            ).label('uris')
        ).label('uris')
    )


def get_native_fields():
    # Native fields (hard-coded into the schema)
    fields = {
        'id': NativeField(
            'id',
            'Dataset UUID',
            Dataset.id
        ),
        'indexed_time': NativeField(
            'indexed_time',
            'When dataset was indexed',
            Dataset.added
        ),
        'indexed_by': NativeField(
            'indexed_by',
            'User who indexed the dataset',
            Dataset.added_by
        ),
        'product': NativeField(
            'product',
            'Product name',
            Product.name
        ),
        'product_id': NativeField(
            'product_id',
            'ID of a dataset type',
            Dataset.product_ref
        ),
        'metadata_type': NativeField(
            'metadata_type',
            'Metadata type name of dataset',
            MetadataType.name
        ),
        'metadata_type_id': NativeField(
            'metadata_type_id',
            'ID of a metadata type',
            Dataset.metadata_type_ref
        ),
        'metadata_doc': NativeField(
            'metadata_doc',
            'Full metadata document',
            Dataset.metadata_doc
        ),
        # Fields that can affect row selection

        # Note that this field is a single uri: selecting it will result in one-result per uri.
        # (ie. duplicate datasets if multiple uris, no dataset if no uris)
        'uri': NativeField(
            'uri',
            "Dataset URI",
            DatasetLocation.uri_body,
            alchemy_expression=DatasetLocation.uri,
            affects_row_selection=True
        ),
    }
    return fields


def get_dataset_fields(metadata_type_definition):
    dataset_section = metadata_type_definition['dataset']

    fields = get_native_fields()
    # "Fixed fields" (not dynamic: defined in metadata type schema)
    fields.update(dict(
        creation_time=DateDocField(
            'creation_time',
            'Time when dataset was created (processed)',
            Dataset.metadata_doc,
            False,
            offset=dataset_section.get('creation_dt') or ['creation_dt']
        ),
        format=SimpleDocField(
            'format',
            'File format (GeoTiff, NetCDF)',
            Dataset.metadata_doc,
            False,
            offset=dataset_section.get('format') or ['format', 'name']
        ),
        label=SimpleDocField(
            'label',
            'Label',
            Dataset.metadata_doc,
            False,
            offset=dataset_section.get('label') or ['label']
        ),
    ))

    # noinspection PyTypeChecker
    fields.update(
        parse_fields(
            dataset_section['search_fields'],
            Dataset.metadata_doc
        )
    )
    return fields


def non_native_fields(mdt_metadata):
    return {
        name: field
        for name, field in get_dataset_fields(mdt_metadata).items()
        if not isinstance(field, NativeField)
    }


def extract_dataset_search_fields(ds_metadata, mdt_metadata):
    """
    :param ds_metdata: A Dataset metadata document
    :param mdt_metadata: The corresponding metadata-type definition document

    :return: A dictionary mapping search field names to (type_name, value) tuples.
    """
    return extract_dataset_fields(ds_metadata, non_native_fields(mdt_metadata))


def extract_dataset_fields(ds_metadata, fields):
    """
    :param ds_metdata: A Dataset metadata document
    :param fields: A dictionary of field names to Field objects

    :return: A dictionary mapping search field names to (type_name, value) tuples.
    """
    result = {}
    for field_name, field in fields.items():
        try:
            fld_type = field.type_name
            raw_val = field.extract(ds_metadata)
            sqla_val = field.search_value_to_alchemy(raw_val)
            result[field_name] = (fld_type, sqla_val)
        except UnindexableValue:
            continue
    return result


class PostgisDbAPI:
    def __init__(self, parentdb, connection):
        self._db = parentdb
        self._connection = connection
        self._sqla_txn = None

    @property
    def in_transaction(self):
        return self._connection.in_transaction()

    def begin(self):
        self._connection.execution_options(isolation_level="REPEATABLE READ")
        self._sqla_txn = self._connection.begin()

    def _end_transaction(self):
        self._sqla_txn = None
        self._connection.execution_options(isolation_level="AUTOCOMMIT")

    def commit(self):
        self._sqla_txn.commit()
        self._end_transaction()

    def rollback(self):
        self._sqla_txn.rollback()
        self._end_transaction()

    def execute(self, command):
        return self._connection.execute(command)

    def insert_dataset(self, metadata_doc, dataset_id, product_id):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type product_id: int
        :return: whether it was inserted
        :rtype: bool
        """
        metadata_subquery = select(Product.metadata_type_ref).where(Product.id == product_id).scalar_subquery()
        ret = self._connection.execute(
            insert(Dataset).values(
                id=dataset_id,
                product_ref=product_id,
                metadata=metadata_doc,
                metadata_type_ref=metadata_subquery
            ).on_conflict_do_nothing(
                index_elements=['id']
            )
        )
        return ret.rowcount > 0

    def insert_dataset_bulk(self, values):
        requested = len(values)
        res = self._connection.execute(
            insert(Dataset), values
        )
        return res.rowcount, requested - res.rowcount

    def update_dataset(self, metadata_doc, dataset_id, product_id):
        """
        Update dataset
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type product_id: int
        """
        res = self._connection.execute(
            update(Dataset).returning(Dataset.id).where(
                Dataset.id == dataset_id
            ).where(
                Dataset.product_ref == product_id
            ).values(
                metadata=metadata_doc
            )
        )
        return res.rowcount > 0

    def insert_dataset_location(self, dataset_id, uri):
        """
        Add a location to a dataset if it is not already recorded.

        Returns True if success, False if this location already existed

        :type dataset_id: str or uuid.UUID
        :type uri: str
        :rtype bool:
        """

        scheme, body = split_uri(uri)

        r = self._connection.execute(
            insert(DatasetLocation).on_conflict_do_nothing(
                index_elements=['uri_scheme', 'uri_body', 'dataset_ref']
            ).values(
                dataset_ref=dataset_id,
                uri_scheme=scheme,
                uri_body=body,
            )
        )

        return r.rowcount > 0

    def insert_dataset_location_bulk(self, values):
        requested = len(values)
        res = self._connection.execute(insert(DatasetLocation), values)
        return res.rowcount, requested - res.rowcount

    def insert_dataset_search(self, search_table, dataset_id, key, value):
        """
        Add/update a search field index entry for a dataset

        Returns True on success

        :type search_table: A DatasetSearch ORM table
        :type dataset_id: str or uuid.UUID
        :type key: The name of the search field
        :type value: The value for the search field for this dataset.
        :rtype bool:
        """
        if isinstance(value, Range):
            value = list(value)
        r = self._connection.execute(
            insert(
                search_table
            ).values(
                dataset_ref=dataset_id,
                search_key=key,
                search_val=value,
            ).on_conflict_do_update(
                index_elements=[search_table.dataset_ref, search_table.search_key],
                set_=dict(search_val=value)
            )
        )
        return r.rowcount > 0

    def insert_dataset_search_bulk(self, search_type, values):
        search_table = search_field_index_map[search_type]
        r = self._connection.execute(insert(search_table).values(values))
        return r.rowcount

    def insert_dataset_spatial(self, dataset_id, crs, extent):
        """
        Add/update a spatial index entry for a dataset

        Returns True on success

        :type dataset_id: str or uuid.UUID
        :type crs: CRS
        :type extent: Geometry
        :rtype bool:
        """
        values = generate_dataset_spatial_values(dataset_id, crs, extent)
        if values is None:
            return False
        SpatialIndex = self._db.spatial_index(crs)  # noqa: N806
        r = self._connection.execute(
            insert(
                SpatialIndex
            ).values(
                **values
            ).on_conflict_do_update(
                index_elements=[SpatialIndex.dataset_ref],
                set_=dict(extent=values["extent"])
            )
        )
        return r.rowcount > 0

    def insert_dataset_spatial_bulk(self, crs, values):
        SpatialIndex = self._db.spatial_index(crs)  # noqa: N806
        r = self._connection.execute(insert(SpatialIndex).values(values))
        return r.rowcount

    def spatial_extent(self, ids, crs):
        SpatialIndex = self._db.spatial_index(crs)  # noqa: N806
        if SpatialIndex is None:
            return None
        query = select(
            func.ST_AsGeoJSON(func.ST_Union(SpatialIndex.extent))
        ).select_from(
            SpatialIndex
        ).where(
            SpatialIndex.dataset_ref.in_(ids)
        )
        result = self._connection.execute(query)
        for r in result:
            extent_json = r[0]
            if extent_json is None:
                return None
            return Geometry(json.loads(extent_json), crs=crs)
        return None

    def contains_dataset(self, dataset_id):
        return bool(
            self._connection.execute(
                select(Dataset.id).where(
                    Dataset.id == dataset_id
                )
            ).fetchone()
        )

    def datasets_intersection(self, dataset_ids):
        """ Compute set intersection: db_dataset_ids & dataset_ids
        """
        return [ds.id for ds in self._connection.execute(
                select(
                    Dataset.id
                ).where(
                    Dataset.id.in_(dataset_ids)
                )
            ).fetchall()
        ]

    def get_datasets_for_location(self, uri, mode=None):
        scheme, body = split_uri(uri)

        if mode is None:
            mode = 'exact' if body.count('#') > 0 else 'prefix'

        if mode == 'exact':
            body_query = DatasetLocation.uri_body == body
        elif mode == 'prefix':
            body_query = DatasetLocation.uri_body.startswith(body)
        else:
            raise ValueError('Unsupported query mode {}'.format(mode))

        return self._connection.execute(
            select(
                *_dataset_select_fields()
            ).join(
                Dataset.locations
            ).where(
                and_(DatasetLocation.uri_scheme == scheme, body_query)
            )
        ).fetchall()

    def all_dataset_ids(self, archived: bool):
        query = select(Dataset.id)
        if archived:
            query = query.where(
                Dataset.archived != None
            )
        else:
            query = query.where(
                Dataset.archived == None
            )
        return self._connection.execute(query).fetchall()

    # Not currently implemented.
    # def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        # r = self._connection.execute(
        #     insert(DatasetSource).on_conflict_do_nothing(
        #         index_elements=['classifier', 'dataset_ref']
        #     ).values(
        #         classifier=classifier,
        #         dataset_ref=dataset_id,
        #         source_dataset_ref=source_dataset_id
        #     )
        # )
        # return r.rowcount > 0

    def archive_dataset(self, dataset_id):
        r = self._connection.execute(
            update(Dataset).where(
                Dataset.id == dataset_id
            ).where(
                Dataset.archived == None
            ).values(
                archived=func.now()
            )
        )
        return r.rowcount > 0

    def restore_dataset(self, dataset_id):
        r = self._connection.execute(
            update(Dataset).where(
                Dataset.id == dataset_id
            ).values(
                archived=None
            )
        )
        return r.rowcount > 0

    def delete_dataset(self, dataset_id):
        self._connection.execute(
            delete(DatasetLocation).where(
                DatasetLocation.dataset_ref == dataset_id
            )
        )
        for table in search_field_indexes.values():
            self._connection.execute(
                delete(table).where(table.dataset_ref == dataset_id)
            )
        for crs in self._db.spatially_indexed_crses():
            SpatialIndex = self._db.spatial_index(crs)  # noqa: N806
            self._connection.execute(
                delete(
                    SpatialIndex
                ).where(
                    SpatialIndex.dataset_ref == dataset_id
                )
            )
        r = self._connection.execute(
            delete(Dataset).where(
                Dataset.id == dataset_id
            )
        )
        return r.rowcount > 0

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            select(*_dataset_select_fields()).where(Dataset.id == dataset_id)
        ).first()

    def get_datasets(self, dataset_ids):
        return self._connection.execute(
            select(*_dataset_select_fields()).where(Dataset.id.in_(dataset_ids))
        ).fetchall()

    def get_derived_datasets(self, dataset_id):
        raise NotImplementedError

    def get_dataset_sources(self, dataset_id):
        raise NotImplementedError

    def search_datasets_by_metadata(self, metadata):
        """
        Find any datasets that have the given metadata.

        :type metadata: dict
        :rtype: dict
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            select(*_dataset_select_fields()).where(Dataset.metadata_doc.contains(metadata))
        ).fetchall()

    def search_products_by_metadata(self, metadata):
        """
        Find any datasets that have the given metadata.

        :type metadata: dict
        :rtype: dict
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            select(Product).where(Product.metadata_doc.contains(metadata))
        ).fetchall()

    @staticmethod
    def _alchemify_expressions(expressions):
        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        return [raw_expr(expression) for expression in expressions]

    def search_datasets_query(self,
                              expressions, source_exprs=None,
                              select_fields=None, with_source_ids=False,
                              limit=None, geom=None):
        """
        :type expressions: Tuple[Expression]
        :type source_exprs: Tuple[Expression]
        :type select_fields: Iterable[PgField]
        :type with_source_ids: bool
        :type limit: int
        :type geom: Geometry
        :rtype: sqlalchemy.Expression
        """
        # TODO: lineage handling and source search
        assert source_exprs is None
        assert not with_source_ids

        if select_fields:
            select_columns = tuple(
                f.alchemy_expression.label(f.name)
                for f in select_fields
            )
        else:
            select_columns = _dataset_select_fields()

        if geom:
            # Check geom CRS - do we have a spatial index for this CRS?
            #           Yes? Use it!
            #           No? Convert to 4326 which we should always have a spatial index for by default
            if not geom.crs:
                raise ValueError("Search geometry must have a CRS")
            SpatialIndex = self._db.spatial_index(geom.crs)   # noqa: N806
            if SpatialIndex is None:
                _LOG.info("No spatial index for crs %s - converting to 4326", geom.crs)
                default_crs = CRS("EPSG:4326")
                geom = geom.to_crs(default_crs)
                SpatialIndex = self._db.spatial_index(default_crs)  # noqa: N806
            geom_sql = geom_alchemy(geom)
            _LOG.info("query geometry = %s (%s)", geom.json, geom.crs)
            spatialquery = func.ST_Intersects(SpatialIndex.extent, geom_sql)
        else:
            spatialquery = None
            SpatialIndex = None  # noqa: N806

        raw_expressions = PostgisDbAPI._alchemify_expressions(expressions)
        join_tables = PostgisDbAPI._join_tables(expressions, select_fields)
        where_expr = and_(Dataset.archived == None, *raw_expressions)
        query = select(*select_columns).select_from(Dataset)
        for joins in join_tables:
            query = query.join(*joins)
        if spatialquery is not None:
            where_expr = and_(where_expr, spatialquery)
            query = query.join(SpatialIndex)
        query = query.where(where_expr).limit(limit)
        return query

    def search_datasets(self, expressions,
                        source_exprs=None, select_fields=None,
                        with_source_ids=False, limit=None,
                        geom=None):
        """
        :type with_source_ids: bool
        :type select_fields: tuple[datacube.drivers.postgis._fields.PgField]
        :type expressions: tuple[datacube.drivers.postgis._fields.PgExpression]
        """
        select_query = self.search_datasets_query(expressions, source_exprs,
                                                  select_fields, with_source_ids,
                                                  limit, geom=geom)
        str_qry = str(select_query)
        _LOG.debug("search_datasets SQL: %s", str_qry)
        return self._connection.execute(select_query)

    def bulk_simple_dataset_search(self, products=None, batch_size=0):
        """
        Perform bulk database reads (e.g. for index cloning)

        Note that this operates with product ids to prevent an unnecessary join to the Product table.

        :param products: Optional iterable of product IDs.  Only fetch nominated products.
        :param batch_size: Number of streamed rows to fetch from database at once.
                           Defaults to zero, which means no streaming.
                           Note streaming is only supported inside a transaction.
        :return: Iterable of tuples of:
                 * Product ID
                 * Dataset metadata document
                 * array of uris
        """
        if batch_size > 0 and not self.in_transaction:
            raise ValueError("Postgresql bulk reads must occur within a transaction.")
        query = select(
            *_dataset_bulk_select_fields()
        ).select_from(Dataset).where(
            Dataset.archived == None
        )
        if products:
            query = query.where(Dataset.product_ref.in_(products))

        if batch_size > 0:
            conn = self._connection.execution_options(stream_results=True, yield_per=batch_size)
        else:
            conn = self._connection
        return conn.execute(query)

    def get_all_lineage(self, batch_size: int):
        """
        Stream all lineage data in bulk (e.g. for index cloning)

        :param batch_size: The number of lineage records to return at once.
        :return: Streamable SQLAlchemy result object.
        """
        if batch_size > 0 and not self.in_transaction:
            raise ValueError("Postgresql bulk reads must occur within a transaction.")
        query = select(
            DatasetLineage.derived_dataset_ref,
            DatasetLineage.classifier,
            DatasetLineage.source_dataset_ref
        )
        return self._connection.execution_options(stream_results=True, yield_per=batch_size).execute(query)

    def insert_lineage_bulk(self, values):
        """
        Insert bulk lineage records (e.g. for index cloning)

        :param values: An array of values dicts for bulk inser
        :return: Tuple[count of rows loaded, count of rows skipped]
        """
        requested = len(values)
        # Simple bulk insert with on_conflict_do_nothing.
        # No need to check referential integrity as this is an external lineage index driver.
        res = self._connection.execute(
            insert(DatasetLineage).on_conflict_do_nothing(),
            values
        )
        return res.rowcount, requested - res.rowcount

    @staticmethod
    def search_unique_datasets_query(expressions, select_fields, limit):
        """
        'unique' here refer to that the query results do not contain datasets
        having the same 'id' more than once.

        We are not dealing with dataset_source table here and we are not joining
        dataset table with dataset_location table. We are aggregating stuff
        in dataset_location per dataset basis if required. It returns the constructed
        query.
        """
        # TODO
        raise NotImplementedError()

    def search_unique_datasets(self, expressions, select_fields=None, limit=None):
        """
        Processes a search query without duplicating datasets.

        'unique' here refer to that the results do not contain datasets having the same 'id'
        more than once. we achieve this by not allowing dataset table to join with
        dataset_location or dataset_source tables. Joining with other tables would not
        result in multiple records per dataset due to the direction of cardinality.
        """
        select_query = self.search_unique_datasets_query(expressions, select_fields, limit)

        return self._connection.execute(select_query)

    def get_duplicates(self, match_fields: Sequence[PgField], expressions: Sequence[PgExpression]) -> Iterable[tuple]:
        # TODO
        if "time" in [f.name for f in match_fields]:
            return self.get_duplicates_with_time(match_fields, expressions)

        group_expressions = tuple(f.alchemy_expression for f in match_fields)
        join_tables = PostgisDbAPI._join_tables(expressions, match_fields)

        query = select(
            func.array_agg(Dataset.id).label("ids"),
            *group_expressions
        ).select_from(Dataset)
        for joins in join_tables:
            query = query.join(*joins)

        query = query.where(
            and_(Dataset.archived == None, *(PostgisDbAPI._alchemify_expressions(expressions)))
        ).group_by(
            *group_expressions
        ).having(
            func.count(Dataset.id) > 1
        )
        return self._connection.execute(query)

    def get_duplicates_with_time(
            self, match_fields: Sequence[PgField], expressions: Sequence[PgExpression]
    ) -> Iterable[tuple]:
        fields = []
        for f in match_fields:
            if f.name == "time":
                time_field = f.expression_with_leniency
            else:
                fields.append(f.alchemy_expression)

        join_tables = PostgisDbAPI._join_tables(expressions, match_fields)

        cols = [Dataset.id, time_field.label('time'), *fields]
        query = select(
            *cols
        ).select_from(Dataset)
        for joins in join_tables:
            query = query.join(*joins)

        query = query.where(
            and_(Dataset.archived == None, *(PostgisDbAPI._alchemify_expressions(expressions)))
        )

        t1 = query.alias("t1")
        t2 = query.alias("t2")

        time_overlap = select(
            t1.c.id,
            text("t1.time * t2.time as time_intersect"),
            *fields
        ).select_from(
            t1.join(
                t2,
                and_(t1.c.time.overlaps(t2.c.time), t1.c.id != t2.c.id)
            )
        )

        query = select(
            func.array_agg(func.distinct(time_overlap.c.id)).label("ids"),
            *fields,
            text("(lower(time_intersect) at time zone 'UTC', upper(time_intersect) at time zone 'UTC') as time")
        ).select_from(
            time_overlap
        ).group_by(
            *fields, text("time_intersect")
        ).having(
            func.count(time_overlap.c.id) > 1
        )
        return self._connection.execute(query)

    def count_datasets(self, expressions):
        """
        :type expressions: tuple[datacube.drivers.postgis._fields.PgExpression]
        :rtype: int
        """

        raw_expressions = self._alchemify_expressions(expressions)

        select_query = (
            select(
                func.count(Dataset.id)
            ).where(
                Dataset.archived == None
            ).where(
                *raw_expressions
            )
        )
        return self._connection.scalar(select_query)

    def count_datasets_through_time(self, start, end, period, time_field, expressions):
        """
        :type period: str
        :type start: datetime.datetime
        :type end: datetime.datetime
        :type expressions: tuple[datacube.drivers.postgis._fields.PgExpression]
        :rtype: list[((datetime.datetime, datetime.datetime), int)]
        """

        results = self._connection.execute(
            self.count_datasets_through_time_query(start, end, period, time_field, expressions)
        )

        for time_period, dataset_count in results:
            # if not time_period.upper_inf:
            yield Range(time_period.lower, time_period.upper), dataset_count

    def count_datasets_through_time_query(self, start, end, period, time_field, expressions):
        raw_expressions = self._alchemify_expressions(expressions)

        start_times = select(
            func.generate_series(start, end, cast(period, INTERVAL)).label('start_time'),
        ).alias('start_times')

        time_range_select = (
            select(
                func.tstzrange(
                    start_times.c.start_time,
                    func.lead(start_times.c.start_time).over()
                ).label('time_period'),
            )
        ).alias('all_time_ranges')

        # Exclude the trailing (end time to infinite) row. Is there a simpler way?
        time_ranges = (
            select(
                time_range_select,
            ).where(
                ~func.upper_inf(time_range_select.c.time_period)
            )
        ).alias('time_ranges')

        count_query = select(func.count('*'))
        join_tables = self._join_tables(expressions)
        for joins in join_tables:
            count_query = count_query.join(*joins)
        count_query = count_query.where(
            and_(
                time_field.alchemy_expression.overlaps(time_ranges.c.time_period),
                Dataset.archived == None,
                *raw_expressions
            )
        )

        return select(time_ranges.c.time_period, count_query.label('dataset_count'))

    def update_search_index(self, product_names: Sequence[str] = [], dsids: Sequence[DSID] = []):
        """
        Update search indexes
        :param product_names: Product names to update
        :param dsids: Dataset IDs to update

        if neither product_names nor dataset ids are supplied, update nothing (N.B. NOT all datasets)

        if both are supplied, both the named products and identified datasets are updated.

        :return:  Number of datasets whose search indexes have been updated.
        """
        if not product_names and not dsids:
            return 0

        ds_query = select(
            Dataset.id,
            Dataset.metadata_doc,
            MetadataType.definition,
        ).select_from(Dataset).join(MetadataType)
        if product_names:
            ds_query = ds_query.join(Product)
        if product_names and dsids:
            ds_query = ds_query.where(
                or_(
                    Product.name.in_(product_names),
                    Dataset.id.in_(dsids)
                )
            )
        elif product_names:
            ds_query = ds_query.where(
                Product.name.in_(product_names)
            )
        elif dsids:
            ds_query = ds_query.where(
                Dataset.id.in_(dsids)
            )
        rowcount = 0
        for result in self._connection.execute(ds_query):
            dsid, ds_metadata, mdt_def = result
            search_field_vals = extract_dataset_search_fields(ds_metadata, mdt_def)
            for field_name, field_info in search_field_vals.items():
                fld_type, fld_val = field_info
                search_idx = search_field_index_map[fld_type]
                self.insert_dataset_search(search_idx, dsid, field_name, fld_val)
            rowcount += 1
        return rowcount

    def update_spindex(self, crs_seq: Sequence[CRS] = [],
                       product_names: Sequence[str] = [],
                       dsids: Sequence[DSID] = []) -> int:
        """
        Update a spatial index
        :param crs: CRSs for Spatial Indexes to update. Default=all indexes
        :param product_names: Product names to update
        :param dsids: Dataset IDs to update

        if neither product_names nor dataset ids are supplied, update for all datasets.

        if both are supplied, both the named products and identified datasets are updated.

        :return:  Number of spatial index entries updated or verified as unindexed.
        """
        verified = 0
        if crs_seq:
            crses = [crs for crs in crs_seq]
        else:
            crses = self._db.spatially_indexed_crses()

        # Update implementation.
        # Design will change, but this method should be fairly low level to be as efficient as possible
        query = select(
            Dataset.id,
            Dataset.metadata_doc["grid_spatial"]["projection"]
        ).select_from(Dataset)
        if product_names:
            query = query.join(Product)
        if product_names and dsids:
            query = query.where(
                or_(
                    Product.name.in_(product_names),
                    Dataset.id.in_(dsids)
                )
            )
        elif product_names:
            query = query.where(
                Product.name.in_(product_names)
            )
        elif dsids:
            query = query.where(
                Dataset.id.in_(dsids)
            )

        def xytuple(o):
            return (o['x'], o['y'])

        for result in self._connection.execute(query):
            dsid = result[0]
            geom = extract_geometry_from_eo3_projection(result[1])
            if not geom:
                verified += 1
                continue
            for crs in crses:
                self.insert_dataset_spatial(dsid, crs, geom)
                verified += 1

        return verified

    @staticmethod
    def _join_tables(expressions=None, fields=None):
        join_args = set()
        if expressions:
            join_args.update(expression.field.dataset_join_args for expression in expressions)
        if fields:
            join_args.update((field.select_alchemy_table,) for field in fields)
        join_args.discard((Dataset.__table__,))
        # Sort simple joins before qualified joins
        return sorted(join_args, key=len)

    def get_product(self, id_):
        return self._connection.execute(
            select(Product).where(Product.id == id_)
        ).first()

    def get_metadata_type(self, id_):
        return self._connection.execute(
            select(MetadataType).where(MetadataType.id == id_)
        ).first()

    def get_product_by_name(self, name):
        return self._connection.execute(
            select(Product).where(Product.name == name)
        ).first()

    def get_metadata_type_by_name(self, name):
        return self._connection.execute(
            select(MetadataType).where(MetadataType.name == name)
        ).first()

    def insert_product(self,
                       name,
                       metadata,
                       metadata_type_id,
                       definition):

        res = self._connection.execute(
            insert(Product).values(
                name=name,
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition
            )
        )

        type_id = res.inserted_primary_key[0]

        return type_id

    def insert_product_bulk(self, values):
        requested = len(values)
        res = self._connection.execute(insert(Product), values)
        return res.rowcount, requested - res.rowcount

    def update_product(self,
                       name,
                       metadata,
                       metadata_type_id,
                       definition,
                       update_metadata_type=False):
        res = self._connection.execute(
            update(Product).returning(Product.id).where(
                Product.name == name
            ).values(
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition
            )
        )
        prod_id = res.first()[0]

        if update_metadata_type:
            if not self._connection.in_transaction():
                raise RuntimeError('Must update metadata types in transaction')

            self._connection.execute(
                update(Dataset).where(
                    Dataset.product_ref == prod_id
                ).values(
                    metadata_type_ref=metadata_type_id,
                )
            )

        return prod_id

    def insert_metadata_type(self, name, definition):
        res = self._connection.execute(
            insert(MetadataType).values(
                name=name,
                definition=definition
            )
        )
        return res.inserted_primary_key[0]

    def insert_metadata_bulk(self, values):
        requested = len(values)
        res = self._connection.execute(
            insert(MetadataType).on_conflict_do_nothing(index_elements=['id']),
            values
        )
        return res.rowcount, requested - res.rowcount

    def update_metadata_type(self, name, definition):
        res = self._connection.execute(
            update(MetadataType).returning(MetadataType.id).where(
                MetadataType.name == name
            ).values(
                name=name,
                definition=definition
            )
        )
        return res.first()[0]

    def _get_active_field_names(fields, metadata_doc):
        for field in fields.values():
            if hasattr(field, 'extract'):
                try:
                    value = field.extract(metadata_doc)
                    if value is not None:
                        yield field.name
                except (AttributeError, KeyError, ValueError):
                    continue

    def get_all_products(self):
        return self._connection.execute(
            select(Product).order_by(Product.name.asc())
        ).fetchall()

    def get_all_product_docs(self):
        return self._connection.execute(
            select(Product.definition)
        )

    def _get_products_for_metadata_type(self, id_):
        return self._connection.execute(
            select(Product).where(
                Product.metadata_type_ref == id_
            ).order_by(
                Product.name.asc()
            )).fetchall()

    def get_all_metadata_types(self):
        return self._connection.execute(select(MetadataType).order_by(MetadataType.name.asc())).fetchall()

    def get_all_metadata_type_defs(self):
        for r in self._connection.execute(select(MetadataType.definition).order_by(MetadataType.name.asc())):
            yield r[0]

    def get_locations(self, dataset_id):
        return [
            record[0]
            for record in self._connection.execute(
                select(
                    DatasetLocation.uri
                ).where(
                    DatasetLocation.dataset_ref == dataset_id
                ).where(
                    DatasetLocation.archived == None
                ).order_by(
                    DatasetLocation.added.desc(),
                    DatasetLocation.id.desc()
                )
            ).fetchall()
        ]

    def get_archived_locations(self, dataset_id):
        """
        Return a list of uris and archived_times for a dataset
        """
        return [
            (location_uri, archived_time)
            for location_uri, archived_time in self._connection.execute(
                select(
                    DatasetLocation.uri, DatasetLocation.archived
                ).where(
                    DatasetLocation.dataset_ref == dataset_id
                ).where(
                    DatasetLocation.archived != None
                ).order_by(
                    DatasetLocation.added.desc()
                )
            ).fetchall()
        ]

    def remove_location(self, dataset_id, uri):
        """
        Remove the given location for a dataset

        :returns bool: Was the location deleted?
        """
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            delete(DatasetLocation).where(
                DatasetLocation.dataset_ref == dataset_id
            ).where(
                DatasetLocation.uri_scheme == scheme
            ).where(
                DatasetLocation.uri_body == body
            )
        )
        return res.rowcount > 0

    def archive_location(self, dataset_id, uri):
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            update(DatasetLocation).where(
                DatasetLocation.dataset_ref == dataset_id
            ).where(
                DatasetLocation.uri_scheme == scheme
            ).where(
                DatasetLocation.uri_body == body
            ).where(
                DatasetLocation.archived == None
            ).values(
                archived=func.now()
            )
        )
        return res.rowcount > 0

    def restore_location(self, dataset_id, uri):
        scheme, body = split_uri(uri)
        res = self._connection.execute(
            update(DatasetLocation).where(
                DatasetLocation.dataset_ref == dataset_id
            ).where(
                DatasetLocation.uri_scheme == scheme
            ).where(
                DatasetLocation.uri_body == body
            ).where(
                DatasetLocation.archived != None
            ).values(
                archived=None
            )
        )
        return res.rowcount > 0

    def __repr__(self):
        return "PostgresDb<connection={!r}>".format(self._connection)

    def list_users(self):
        result = self._connection.execute(text("""
            select
                group_role.rolname as role_name,
                user_role.rolname as user_name,
                pg_catalog.shobj_description(user_role.oid, 'pg_authid') as description
            from pg_roles group_role
            inner join pg_auth_members am on am.roleid = group_role.oid
            inner join pg_roles user_role on am.member = user_role.oid
            where (group_role.rolname like 'odc_%%') and not (user_role.rolname like 'odc_%%')
            order by group_role.oid asc, user_role.oid asc;
        """))
        for row in result:
            yield _core.from_pg_role(row.role_name), row.user_name, row.description

    def create_user(self, username, password, role, description=None):
        pg_role = _core.to_pg_role(role)
        username = escape_pg_identifier(self._connection, username)
        sql = text('create user {username} password :password in role {role}'.format(username=username, role=pg_role))
        self._connection.execute(sql, {"password": password})
        if description:
            sql = text('comment on role {username} is :description'.format(username=username))
            self._connection.execute(sql, {"description": description})

    def drop_users(self, users: Iterable[str]) -> str:
        for username in users:
            sql = text('drop role {username}'.format(username=escape_pg_identifier(self._connection, username)))
            self._connection.execute(sql)

    def grant_role(self, role, users):
        # type: (str, Iterable[str]) -> None
        """
        Grant a role to a user.
        """
        pg_role = _core.to_pg_role(role)

        for user in users:
            if not _core.has_role(self._connection, user):
                raise ValueError('Unknown user %r' % user)

        _core.grant_role(self._connection, pg_role, users)

    def insert_home(self, home, ids, allow_updates):
        """
        Set home for multiple IDs (but one home value)

        :param home: The home value to set
        :param ids: The IDs to set it for
        :param allow_updates: If False only inserts are allowed
        :return: number of database records updated or added.
        """
        values = [
            {"dataset_ref": id_, "home": home}
            for id_ in ids
        ]
        qry = insert(DatasetHome)
        if allow_updates:
            qry = qry.on_conflict_do_update(
                index_elements=["dataset_ref"],
                set_={"home": home},
                where=(DatasetHome.home != home))
        try:
            res = self._connection.execute(
                qry,
                values
            )
            return res.rowcount
        except IntegrityError:
            return 0

    def delete_home(self, ids):
        """
        Delete the home value for the specified IDs

        :param ids: The IDs to delete home for
        :return: The number of hone records deleted from the databes
        """
        res = self._connection.execute(
            delete(DatasetHome).where(DatasetHome.dataset_ref.in_(ids))
        )
        return res.rowcount

    def select_homes(self, ids):
        """
        Find homes for IDs.

        :param ids: Iterable of IDs
        :return: Mapping of ID to home string for IDs found in database.
        """
        results = self._connection.execute(
            select(DatasetHome).where(DatasetHome.dataset_ref.in_(ids))
        )
        return {
            row.dataset_ref: row.home
            for row in results
        }

    def get_all_relations(self, dsids: Iterable[uuid.UUID]) -> Iterable[LineageRelation]:
        """
        Fetch all lineage relations in the database involving a set on dataset IDs.

        :param dsids: Iterable of dataset IDs
        :return: Iterable of LineageRelation objects.
        """
        results = self._connection.execute(
            select(DatasetLineage).where(or_(
                DatasetLineage.derived_dataset_ref.in_(dsids),
                DatasetLineage.source_dataset_ref.in_(dsids)
            ))
        )
        for rel in results:
            yield LineageRelation(classifier=rel.classifier,
                                  source_id=rel.source_dataset_ref,
                                  derived_id=rel.derived_dataset_ref)

    def write_relations(self, relations: Iterable[LineageRelation], allow_updates: bool):
        """
        Write a set of LineageRelation objects to the database.

        :param relations: An Iterable of LineageRelation objects
        :param allow_updates: if False, only allow adding new relations, not updating old ones.
        :return: Count of database rows affected
        """
        if allow_updates:
            by_classifier = {}
            for rel in relations:
                db_repr = {
                    "derived_dataset_ref": rel.derived_id,
                    "source_dataset_ref": rel.source_id,
                    "classifier": rel.classifier
                }
                if rel.classifier in by_classifier:
                    by_classifier[rel.classifier].append(db_repr)
                else:
                    by_classifier[rel.classifier] = [db_repr]
                updates = 0
                for classifier, values in by_classifier.items():
                    qry = insert(DatasetLineage).on_conflict_do_update(
                        index_elements=["derived_dataset_ref", "source_dataset_ref"],
                        set_={"classifier": classifier},
                        where=(DatasetLineage.classifier != classifier))
                    res = self._connection.execute(qry, values)
                    updates += res.rowcount
                return updates
        else:
            values = [
                {
                    "derived_dataset_ref": rel.derived_id,
                    "source_dataset_ref": rel.source_id,
                    "classifier": rel.classifier
                }
                for rel in relations
            ]
            qry = insert(DatasetLineage)
            try:
                res = self._connection.execute(
                    qry, values
                )
                return res.rowcount
            except IntegrityError:
                return 0

    def load_lineage_relations(self,
                               roots: Iterable[uuid.UUID],
                               direction: LineageDirection,
                               depth: int,
                               ids_so_far: Optional[Set[uuid.UUID]] = None) -> Iterable[LineageRelation]:
        """
        Read from the database all indexed LineageRelation objects required to build all LineageTrees with
        the given roots, direction and depth.

        :param roots: Iterable of root dataset ids
        :param direction: tree direction
        :param depth: Maximum tree depth - zero indicates unlimited depth.
        :param ids_so_far: Used for maintaining state through recursion - expected to be None on initial call
        :return: Iterable of LineageRelation objects read from database
        """
        # Naive manually-recursive initial implementation.
        # TODO: Reimplement using WITH RECURSIVE query
        if ids_so_far is None:
            ids_so_far = set(roots)
        qry = select(DatasetLineage)
        if direction == LineageDirection.SOURCES:
            qry = qry.where(DatasetLineage.derived_dataset_ref.in_(roots))
        else:
            qry = qry.where(DatasetLineage.source_dataset_ref.in_(roots))
        relations = []
        next_lvl_ids = set()
        results = self._connection.execute(qry)
        for row in results:
            rel = LineageRelation(classifier=row.classifier,
                                  source_id=row.source_dataset_ref,
                                  derived_id=row.derived_dataset_ref)
            relations.append(rel)
            if direction == LineageDirection.SOURCES:
                next_id = rel.source_id
            else:
                next_id = rel.derived_id
            if next_id not in ids_so_far:
                next_lvl_ids.add(next_id)
                ids_so_far.add(next_id)
        next_depth = depth - 1
        recurse = True
        if depth == 0:
            next_depth = 0
        elif depth == 1:
            recurse = False
        if recurse and next_lvl_ids:
            relations.extend(self.load_lineage_relations(next_lvl_ids, direction, next_depth, ids_so_far))
        return relations

    def remove_lineage_relations(self,
                                 ids: Iterable[DSID],
                                 direction: LineageDirection) -> int:
        """
        Remove lineage relations from the provided ids in the specified direction.

        Note no depth parameter - depth is effectively always 1.

        :param ids: Iterable of IDs to remove lineage information for.
        :param direction: Remove the source or derived lineage relation records
        :return: Return number of relation records deleted.
        """
        qry = delete(DatasetLineage)
        if direction == LineageDirection.SOURCES:
            qry = qry.where(DatasetLineage.derived_dataset_ref.in_(ids))
        else:
            qry = qry.where(DatasetLineage.source_dataset_ref.in_(ids))
        results = self._connection.execute(qry)
        return results.rowcount

    def temporal_extent_by_prod(self, product_id: int) -> tuple[datetime.datetime, datetime.datetime]:
        query = self.temporal_extent_full().where(Dataset.product_ref == product_id)
        res = self._connection.execute(query)
        return res.first()

    def temporal_extent_by_ids(self, ids: Iterable[DSID]) -> tuple[datetime.datetime, datetime.datetime]:
        query = self.temporal_extent_full().where(Dataset.id.in_(ids))
        res = self._connection.execute(query)
        return res.first()

    def temporal_extent_full(self) -> Select:
        # Hardcode eo3 standard time locations - do not use this approach in a legacy index driver.
        time_min = DateDocField('aquisition_time_min',
                                'Min of time when dataset was acquired',
                                Dataset.metadata_doc,
                                False,  # is it indexed
                                offset=[
                                    ['properties', 'dtr:start_datetime'],
                                    ['properties', 'datetime']
                                ],
                                selection='least')
        time_max = DateDocField('aquisition_time_max',
                                'Max of time when dataset was acquired',
                                Dataset.metadata_doc,
                                False,  # is it indexed
                                offset=[
                                    ['properties', 'dtr:end_datetime'],
                                    ['properties', 'datetime']
                                ],
                                selection='greatest')
        return select(
            func.min(time_min.alchemy_expression), func.max(time_max.alchemy_expression)
        )
