# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods,too-many-lines

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Persistence API implementation for postgis.
"""

import logging
import uuid  # noqa: F401
from sqlalchemy import cast
from sqlalchemy import delete
from sqlalchemy import select, text, bindparam, and_, or_, func, literal, distinct
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.exc import IntegrityError
from typing import Iterable, Tuple

from datacube.index.exceptions import MissingRecordError
from datacube.index.fields import OrExpression
from datacube.model import Range
from . import _core
from . import _dynamic as dynamic
from ._fields import parse_fields, Expression, PgField, PgExpression  # noqa: F401
from ._fields import NativeField, DateDocField, SimpleDocField
from ._schema import DATASET, DATASET_SOURCE, METADATA_TYPE, DATASET_LOCATION, PRODUCT
from .sql import escape_pg_identifier


def _dataset_uri_field(table):
    return table.c.uri_scheme + ':' + table.c.uri_body


# Fields for selecting dataset with uris
# Need to alias the table, as queries may join the location table for filtering.
SELECTED_DATASET_LOCATION = DATASET_LOCATION.alias('selected_dataset_location')
_DATASET_SELECT_FIELDS = (
    DATASET,
    # All active URIs, from newest to oldest
    func.array(
        select([
            _dataset_uri_field(SELECTED_DATASET_LOCATION)
        ]).where(
            and_(
                SELECTED_DATASET_LOCATION.c.dataset_ref == DATASET.c.id,
                SELECTED_DATASET_LOCATION.c.archived == None
            )
        ).order_by(
            SELECTED_DATASET_LOCATION.c.added.desc(),
            SELECTED_DATASET_LOCATION.c.id.desc()
        ).label('uris')
    ).label('uris')
)

PGCODE_UNIQUE_CONSTRAINT = '23505'
PGCODE_FOREIGN_KEY_VIOLATION = '23503'

_LOG = logging.getLogger(__name__)


def _split_uri(uri):
    """
    Split the scheme and the remainder of the URI.

    """
    idx = uri.find(':')
    if idx < 0:
        raise ValueError("Not a URI")

    return uri[:idx], uri[idx+1:]


def get_native_fields():
    # Native fields (hard-coded into the schema)
    fields = {
        'id': NativeField(
            'id',
            'Dataset UUID',
            DATASET.c.id
        ),
        'indexed_time': NativeField(
            'indexed_time',
            'When dataset was indexed',
            DATASET.c.added
        ),
        'indexed_by': NativeField(
            'indexed_by',
            'User who indexed the dataset',
            DATASET.c.added_by
        ),
        'product': NativeField(
            'product',
            'Product name',
            PRODUCT.c.name
        ),
        'product_id': NativeField(
            'product_id',
            'ID of a dataset type',
            DATASET.c.product_ref
        ),
        'metadata_type': NativeField(
            'metadata_type',
            'Metadata type name of dataset',
            METADATA_TYPE.c.name
        ),
        'metadata_type_id': NativeField(
            'metadata_type_id',
            'ID of a metadata type',
            DATASET.c.metadata_type_ref
        ),
        'metadata_doc': NativeField(
            'metadata_doc',
            'Full metadata document',
            DATASET.c.metadata
        ),
        # Fields that can affect row selection

        # Note that this field is a single uri: selecting it will result in one-result per uri.
        # (ie. duplicate datasets if multiple uris, no dataset if no uris)
        'uri': NativeField(
            'uri',
            "Dataset URI",
            DATASET_LOCATION.c.uri_body,
            alchemy_expression=_dataset_uri_field(DATASET_LOCATION),
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
            DATASET.c.metadata,
            False,
            offset=dataset_section.get('creation_dt') or ['creation_dt']
        ),
        format=SimpleDocField(
            'format',
            'File format (GeoTiff, NetCDF)',
            DATASET.c.metadata,
            False,
            offset=dataset_section.get('format') or ['format', 'name']
        ),
        label=SimpleDocField(
            'label',
            'Label',
            DATASET.c.metadata,
            False,
            offset=dataset_section.get('label') or ['label']
        ),
    ))

    # noinspection PyTypeChecker
    fields.update(
        parse_fields(
            dataset_section['search_fields'],
            DATASET.c.metadata
        )
    )
    return fields


class PostgisDbAPI(object):
    def __init__(self, connection):
        self._connection = connection

    @property
    def in_transaction(self):
        return self._connection.in_transaction()

    def rollback(self):
        self._connection.execute(text('ROLLBACK'))

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
        product_ref = bindparam('product_ref')
        ret = self._connection.execute(
            insert(DATASET).from_select(
                ['id', 'product_ref', 'metadata_type_ref', 'metadata'],
                select([
                    bindparam('id'), product_ref,
                    select([
                        PRODUCT.c.metadata_type_ref
                    ]).where(
                        PRODUCT.c.id == product_ref
                    ).label('metadata_type_ref'),
                    bindparam('metadata', type_=JSONB)
                ])
            ).on_conflict_do_nothing(
                index_elements=['id']
            ),
            id=dataset_id,
            product_ref=product_id,
            metadata=metadata_doc
        )
        return ret.rowcount > 0

    def update_dataset(self, metadata_doc, dataset_id, product_id):
        """
        Update dataset
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type product_id: int
        """
        res = self._connection.execute(
            DATASET.update().returning(DATASET.c.id).where(
                and_(
                    DATASET.c.id == dataset_id,
                    DATASET.c.product_ref == product_id
                )
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

        scheme, body = _split_uri(uri)

        r = self._connection.execute(
            insert(DATASET_LOCATION).on_conflict_do_nothing(
                index_elements=['uri_scheme', 'uri_body', 'dataset_ref']
            ),
            dataset_ref=dataset_id,
            uri_scheme=scheme,
            uri_body=body,
        )

        return r.rowcount > 0

    def contains_dataset(self, dataset_id):
        return bool(
            self._connection.execute(
                select(
                    [DATASET.c.id]
                ).where(
                    DATASET.c.id == dataset_id
                )
            ).fetchone()
        )

    def datasets_intersection(self, dataset_ids):
        """ Compute set intersection: db_dataset_ids & dataset_ids
        """
        return [r[0]
                for r in self._connection.execute(select(
                    [DATASET.c.id]
                ).where(
                    DATASET.c.id.in_(dataset_ids)
                )).fetchall()]

    def get_datasets_for_location(self, uri, mode=None):
        scheme, body = _split_uri(uri)

        if mode is None:
            mode = 'exact' if body.count('#') > 0 else 'prefix'

        if mode == 'exact':
            body_query = DATASET_LOCATION.c.uri_body == body
        elif mode == 'prefix':
            body_query = DATASET_LOCATION.c.uri_body.startswith(body)
        else:
            raise ValueError('Unsupported query mode {}'.format(mode))

        return self._connection.execute(
            select(
                _DATASET_SELECT_FIELDS
            ).select_from(
                DATASET_LOCATION.join(DATASET)
            ).where(
                and_(DATASET_LOCATION.c.uri_scheme == scheme, body_query)
            )
        ).fetchall()

    def all_dataset_ids(self, archived: bool):
        query = select(
            DATASET.c.id  # type: ignore[arg-type]
        ).select_from(
            DATASET
        )
        if archived:
            query = query.where(
                DATASET.c.archived != None
            )
        else:
            query = query.where(
                DATASET.c.archived == None
            )
        return self._connection.execute(query).fetchall()

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        try:
            r = self._connection.execute(
                insert(DATASET_SOURCE).on_conflict_do_nothing(
                    index_elements=['classifier', 'dataset_ref']
                ),
                classifier=classifier,
                dataset_ref=dataset_id,
                source_dataset_ref=source_dataset_id
            )
            return r.rowcount > 0
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_FOREIGN_KEY_VIOLATION:
                raise MissingRecordError("Referenced source dataset doesn't exist")
            raise

    def archive_dataset(self, dataset_id):
        self._connection.execute(
            DATASET.update().where(
                DATASET.c.id == dataset_id
            ).where(
                DATASET.c.archived == None
            ).values(
                archived=func.now()
            )
        )

    def restore_dataset(self, dataset_id):
        self._connection.execute(
            DATASET.update().where(
                DATASET.c.id == dataset_id
            ).values(
                archived=None
            )
        )

    def delete_dataset(self, dataset_id):
        self._connection.execute(
            DATASET_LOCATION.delete().where(
                DATASET_LOCATION.c.dataset_ref == dataset_id
            )
        )
        self._connection.execute(
            DATASET_SOURCE.delete().where(
                DATASET_SOURCE.c.dataset_ref == dataset_id
            )
        )
        self._connection.execute(
            DATASET.delete().where(
                DATASET.c.id == dataset_id
            )
        )

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.id == dataset_id)
        ).first()

    def get_datasets(self, dataset_ids):
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.id.in_(dataset_ids))
        ).fetchall()

    def get_derived_datasets(self, dataset_id):
        return self._connection.execute(
            select(
                _DATASET_SELECT_FIELDS
            ).select_from(
                DATASET.join(DATASET_SOURCE, DATASET.c.id == DATASET_SOURCE.c.dataset_ref)
            ).where(
                DATASET_SOURCE.c.source_dataset_ref == dataset_id
            )
        ).fetchall()

    def get_dataset_sources(self, dataset_id):
        # recursively build the list of (dataset_ref, source_dataset_ref) pairs starting from dataset_id
        # include (dataset_ref, NULL) [hence the left join]
        sources = select(
            [DATASET.c.id.label('dataset_ref'),
             DATASET_SOURCE.c.source_dataset_ref,
             DATASET_SOURCE.c.classifier]
        ).select_from(
            DATASET.join(DATASET_SOURCE,
                         DATASET.c.id == DATASET_SOURCE.c.dataset_ref,
                         isouter=True)
        ).where(
            DATASET.c.id == dataset_id
        ).cte(name="sources", recursive=True)

        sources = sources.union_all(
            select(
                [sources.c.source_dataset_ref.label('dataset_ref'),
                 DATASET_SOURCE.c.source_dataset_ref,
                 DATASET_SOURCE.c.classifier]
            ).select_from(
                sources.join(DATASET_SOURCE,
                             sources.c.source_dataset_ref == DATASET_SOURCE.c.dataset_ref,
                             isouter=True)
            ).where(sources.c.source_dataset_ref != None))

        # turn the list of pairs into adjacency list (dataset_ref, [source_dataset_ref, ...])
        # some source_dataset_ref's will be NULL
        aggd = select(
            [sources.c.dataset_ref,
             func.array_agg(sources.c.source_dataset_ref).label('sources'),
             func.array_agg(sources.c.classifier).label('classes')]
        ).group_by(sources.c.dataset_ref).alias('aggd')

        # join the adjacency list with datasets table
        query = select(
            _DATASET_SELECT_FIELDS + (aggd.c.sources, aggd.c.classes)
        ).select_from(aggd.join(DATASET, DATASET.c.id == aggd.c.dataset_ref))

        return self._connection.execute(query).fetchall()

    def search_datasets_by_metadata(self, metadata):
        """
        Find any datasets that have the given metadata.

        :type metadata: dict
        :rtype: dict
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.metadata.contains(metadata))
        ).fetchall()

    @staticmethod
    def _alchemify_expressions(expressions):
        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        return [raw_expr(expression) for expression in expressions]

    @staticmethod
    def search_datasets_query(expressions, source_exprs=None,
                              select_fields=None, with_source_ids=False, limit=None):
        """
        :type expressions: Tuple[Expression]
        :type source_exprs: Tuple[Expression]
        :type select_fields: Iterable[PgField]
        :type with_source_ids: bool
        :type limit: int
        :rtype: sqlalchemy.Expression
        """

        if select_fields:
            select_columns = tuple(
                f.alchemy_expression.label(f.name)
                for f in select_fields
            )
        else:
            select_columns = _DATASET_SELECT_FIELDS

        if with_source_ids:
            # Include the IDs of source datasets
            select_columns += (
                select(
                    (func.array_agg(DATASET_SOURCE.c.source_dataset_ref),)
                ).select_from(
                    DATASET_SOURCE
                ).where(
                    DATASET_SOURCE.c.dataset_ref == DATASET.c.id
                ).group_by(
                    DATASET_SOURCE.c.dataset_ref
                ).label('dataset_refs'),
            )

        raw_expressions = PostgisDbAPI._alchemify_expressions(expressions)
        from_expression = PostgisDbAPI._from_expression(DATASET, expressions, select_fields)
        where_expr = and_(DATASET.c.archived == None, *raw_expressions)

        if not source_exprs:
            return (
                select(
                    select_columns
                ).select_from(
                    from_expression
                ).where(
                    where_expr
                ).limit(
                    limit
                )
            )
        base_query = (
            select(
                select_columns + (DATASET_SOURCE.c.source_dataset_ref,
                                  literal(1).label('distance'),
                                  DATASET_SOURCE.c.classifier.label('path'))
            ).select_from(
                from_expression.join(DATASET_SOURCE, DATASET.c.id == DATASET_SOURCE.c.dataset_ref)
            ).where(
                where_expr
            )
        ).cte(name="base_query", recursive=True)

        recursive_query = base_query.union_all(
            select(
                [col for col in base_query.columns
                 if col.name not in ['source_dataset_ref', 'distance', 'path']
                 ] + [
                    DATASET_SOURCE.c.source_dataset_ref,
                    (base_query.c.distance + 1).label('distance'),
                    (base_query.c.path + '.' + DATASET_SOURCE.c.classifier).label('path')
                ]
            ).select_from(
                base_query.join(
                    DATASET_SOURCE, base_query.c.source_dataset_ref == DATASET_SOURCE.c.dataset_ref
                )
            )
        )

        return (
            select(
                [distinct(recursive_query.c.id)
                 ] + [
                    col for col in recursive_query.columns
                    if col.name not in ['id', 'source_dataset_ref', 'distance', 'path']]
            ).select_from(
                recursive_query.join(DATASET, DATASET.c.id == recursive_query.c.source_dataset_ref)
            ).where(
                and_(DATASET.c.archived == None, *PostgisDbAPI._alchemify_expressions(source_exprs))
            ).limit(
                limit
            )
        )

    def search_datasets(self, expressions,
                        source_exprs=None, select_fields=None,
                        with_source_ids=False, limit=None):
        """
        :type with_source_ids: bool
        :type select_fields: tuple[datacube.drivers.postgis._fields.PgField]
        :type expressions: tuple[datacube.drivers.postgis._fields.PgExpression]
        """
        select_query = self.search_datasets_query(expressions, source_exprs,
                                                  select_fields, with_source_ids, limit)
        return self._connection.execute(select_query)

    @staticmethod
    def search_unique_datasets_query(expressions, select_fields, limit):
        """
        'unique' here refer to that the query results do not contain datasets
        having the same 'id' more than once.

        We are not dealing with dataset_source table here and we are not joining
        dataset table with dataset_location table. We are aggregating stuff
        in dataset_location per dataset basis if required. It returns the construted
        query.
        """

        # expressions involving DATASET_SOURCE cannot not done for now
        for expression in expressions:
            assert expression.field.required_alchemy_table != DATASET_SOURCE, \
                'Joins with dataset_source cannot be done for this query'

        # expressions involving 'uri' and 'uris' will be handled different
        expressions = [expression for expression in expressions
                       if expression.field.required_alchemy_table != DATASET_LOCATION]

        if select_fields:
            select_columns = []
            for field in select_fields:
                if field.name in {'uri', 'uris'}:
                    # All active URIs, from newest to oldest
                    uris_field = func.array(
                        select([
                            _dataset_uri_field(SELECTED_DATASET_LOCATION)
                        ]).where(
                            and_(
                                SELECTED_DATASET_LOCATION.c.dataset_ref == DATASET.c.id,
                                SELECTED_DATASET_LOCATION.c.archived == None
                            )
                        ).order_by(
                            SELECTED_DATASET_LOCATION.c.added.desc(),
                            SELECTED_DATASET_LOCATION.c.id.desc()
                        ).label('uris')
                    ).label('uris')
                    select_columns.append(uris_field)
                else:
                    select_columns.append(field.alchemy_expression.label(field.name))
        else:
            select_columns = _DATASET_SELECT_FIELDS

        raw_expressions = PostgisDbAPI._alchemify_expressions(expressions)

        # We don't need 'DATASET_LOCATION table in the from expression
        select_fields_ = [field for field in select_fields if field.name not in {'uri', 'uris'}]

        from_expression = PostgisDbAPI._from_expression(DATASET, expressions, select_fields_)
        where_expr = and_(DATASET.c.archived == None, *raw_expressions)

        return (
            select(
                select_columns
            ).select_from(
                from_expression
            ).where(
                where_expr
            ).limit(
                limit
            )
        )

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

    def get_duplicates(self, match_fields, expressions):
        # type: (Tuple[PgField], Tuple[PgExpression]) -> Iterable[tuple]
        group_expressions = tuple(f.alchemy_expression for f in match_fields)

        select_query = select(
            (func.array_agg(DATASET.c.id),) + group_expressions
        ).select_from(
            PostgisDbAPI._from_expression(DATASET, expressions, match_fields)
        ).where(
            and_(DATASET.c.archived == None, *(PostgisDbAPI._alchemify_expressions(expressions)))
        ).group_by(
            *group_expressions
        ).having(
            func.count(DATASET.c.id) > 1
        )
        return self._connection.execute(select_query)

    def count_datasets(self, expressions):
        """
        :type expressions: tuple[datacube.drivers.postgis._fields.PgExpression]
        :rtype: int
        """

        raw_expressions = self._alchemify_expressions(expressions)

        select_query = (
            select(
                [func.count('*')]
            ).select_from(
                self._from_expression(DATASET, expressions)
            ).where(
                and_(DATASET.c.archived == None, *raw_expressions)
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

        start_times = select((
            func.generate_series(start, end, cast(period, INTERVAL)).label('start_time'),
        )).alias('start_times')

        time_range_select = (
            select((
                func.tstzrange(
                    start_times.c.start_time,
                    func.lead(start_times.c.start_time).over()
                ).label('time_period'),
            ))
        ).alias('all_time_ranges')

        # Exclude the trailing (end time to infinite) row. Is there a simpler way?
        time_ranges = (
            select((
                time_range_select,
            )).where(
                ~func.upper_inf(time_range_select.c.time_period)
            )
        ).alias('time_ranges')

        count_query = (
            select(
                (func.count('*'),)
            ).select_from(
                self._from_expression(DATASET, expressions)
            ).where(
                and_(
                    time_field.alchemy_expression.overlaps(time_ranges.c.time_period),
                    DATASET.c.archived == None,
                    *raw_expressions
                )
            )
        )

        return select((time_ranges.c.time_period, count_query.label('dataset_count')))

    @staticmethod
    def _from_expression(source_table, expressions=None, fields=None):
        join_tables = set()
        if expressions:
            join_tables.update(expression.field.required_alchemy_table for expression in expressions)
        if fields:
            join_tables.update(field.required_alchemy_table for field in fields)
        join_tables.discard(source_table)

        table_order_hack = [DATASET_SOURCE, DATASET_LOCATION, DATASET, PRODUCT, METADATA_TYPE]

        from_expression = source_table
        for table in table_order_hack:
            if table in join_tables:
                from_expression = from_expression.join(table)
        return from_expression

    def get_product(self, id_):
        return self._connection.execute(
            PRODUCT.select().where(PRODUCT.c.id == id_)
        ).first()

    def get_metadata_type(self, id_):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.id == id_)
        ).first()

    def get_product_by_name(self, name):
        return self._connection.execute(
            PRODUCT.select().where(PRODUCT.c.name == name)
        ).first()

    def get_metadata_type_by_name(self, name):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.name == name)
        ).first()

    def insert_product(self,
                       name,
                       metadata,
                       metadata_type_id,
                       search_fields,
                       definition,
                       concurrently=True):

        res = self._connection.execute(
            PRODUCT.insert().values(
                name=name,
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition
            )
        )

        type_id = res.inserted_primary_key[0]

        # Initialise search fields.
        self._setup_product_fields(type_id, name, search_fields, definition['metadata'],
                                   concurrently=concurrently)
        return type_id

    def update_product(self,
                       name,
                       metadata,
                       metadata_type_id,
                       search_fields,
                       definition, update_metadata_type=False, concurrently=False):
        res = self._connection.execute(
            PRODUCT.update().returning(PRODUCT.c.id).where(
                PRODUCT.c.name == name
            ).values(
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition
            )
        )
        type_id = res.first()[0]

        if update_metadata_type:
            if not self._connection.in_transaction():
                raise RuntimeError('Must update metadata types in transaction')

            self._connection.execute(
                DATASET.update().where(
                    DATASET.c.product_ref == type_id
                ).values(
                    metadata_type_ref=metadata_type_id,
                )
            )

        # Initialise search fields.
        self._setup_product_fields(type_id, name, search_fields, definition['metadata'],
                                   concurrently=concurrently,
                                   rebuild_view=True)
        return type_id

    def insert_metadata_type(self, name, definition, concurrently=False):
        res = self._connection.execute(
            METADATA_TYPE.insert().values(
                name=name,
                definition=definition
            )
        )
        type_id = res.inserted_primary_key[0]

        search_fields = get_dataset_fields(definition)
        self._setup_metadata_type_fields(
            type_id, name, search_fields, concurrently=concurrently
        )

    def update_metadata_type(self, name, definition, concurrently=False):
        res = self._connection.execute(
            METADATA_TYPE.update().returning(METADATA_TYPE.c.id).where(
                METADATA_TYPE.c.name == name
            ).values(
                name=name,
                definition=definition
            )
        )
        type_id = res.first()[0]

        search_fields = get_dataset_fields(definition)
        self._setup_metadata_type_fields(
            type_id, name, search_fields,
            concurrently=concurrently,
            rebuild_views=True,
        )

        return type_id

    def check_dynamic_fields(self, concurrently=False, rebuild_views=False, rebuild_indexes=False):
        _LOG.info('Checking dynamic views/indexes. (rebuild views=%s, indexes=%s)', rebuild_views, rebuild_indexes)

        search_fields = {}

        for metadata_type in self.get_all_metadata_types():
            fields = get_dataset_fields(metadata_type['definition'])
            search_fields[metadata_type['id']] = fields
            self._setup_metadata_type_fields(
                metadata_type['id'],
                metadata_type['name'],
                fields,
                rebuild_indexes=rebuild_indexes,
                rebuild_views=rebuild_views,
                concurrently=concurrently,
            )

    def _setup_metadata_type_fields(self, id_, name, fields,
                                    rebuild_indexes=False, rebuild_views=False, concurrently=True):
        # Metadata fields are no longer used (all queries are per-dataset-type): exclude all.
        # This will have the effect of removing any old indexes that still exist.
        exclude_fields = tuple(fields)

        dataset_filter = and_(DATASET.c.archived == None, DATASET.c.metadata_type_ref == id_)
        dynamic.check_dynamic_fields(self._connection, concurrently, dataset_filter,
                                     exclude_fields, fields, name,
                                     rebuild_indexes=rebuild_indexes, rebuild_view=rebuild_views)

        for product in self._get_products_for_metadata_type(id_):
            self._setup_product_fields(
                product['id'],
                product['name'],
                fields,
                product['definition']['metadata'],
                rebuild_view=rebuild_views,
                rebuild_indexes=rebuild_indexes,
                concurrently=concurrently
            )

    def _setup_product_fields(self, id_, name, fields, metadata_doc,
                              rebuild_indexes=False, rebuild_view=False, concurrently=True):
        dataset_filter = and_(DATASET.c.archived == None, DATASET.c.product_ref == id_)
        excluded_field_names = tuple(self._get_active_field_names(fields, metadata_doc))

        dynamic.check_dynamic_fields(self._connection, concurrently, dataset_filter,
                                     excluded_field_names, fields, name,
                                     rebuild_indexes=rebuild_indexes, rebuild_view=rebuild_view)

    @staticmethod
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
            PRODUCT.select().order_by(PRODUCT.c.name.asc())
        ).fetchall()

    def _get_products_for_metadata_type(self, id_):
        return self._connection.execute(
            PRODUCT.select(
            ).where(
                PRODUCT.c.metadata_type_ref == id_
            ).order_by(
                PRODUCT.c.name.asc()
            )).fetchall()

    def get_all_metadata_types(self):
        return self._connection.execute(METADATA_TYPE.select().order_by(METADATA_TYPE.c.name.asc())).fetchall()

    def get_locations(self, dataset_id):
        return [
            record[0]
            for record in self._connection.execute(
                select([
                    _dataset_uri_field(DATASET_LOCATION)
                ]).where(
                    and_(DATASET_LOCATION.c.dataset_ref == dataset_id, DATASET_LOCATION.c.archived == None)
                ).order_by(
                    DATASET_LOCATION.c.added.desc(),
                    DATASET_LOCATION.c.id.desc()
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
                select([
                    _dataset_uri_field(DATASET_LOCATION), DATASET_LOCATION.c.archived
                ]).where(
                    and_(DATASET_LOCATION.c.dataset_ref == dataset_id, DATASET_LOCATION.c.archived != None)
                ).order_by(
                    DATASET_LOCATION.c.added.desc()
                )
            ).fetchall()
        ]

    def remove_location(self, dataset_id, uri):
        """
        Remove the given location for a dataset

        :returns bool: Was the location deleted?
        """
        scheme, body = _split_uri(uri)
        res = self._connection.execute(
            delete(DATASET_LOCATION).where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                )
            )
        )
        return res.rowcount > 0

    def archive_location(self, dataset_id, uri):
        scheme, body = _split_uri(uri)
        res = self._connection.execute(
            DATASET_LOCATION.update().where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                    DATASET_LOCATION.c.archived == None,
                )
            ).values(
                archived=func.now()
            )
        )
        return res.rowcount > 0

    def restore_location(self, dataset_id, uri):
        scheme, body = _split_uri(uri)
        res = self._connection.execute(
            DATASET_LOCATION.update().where(
                and_(
                    DATASET_LOCATION.c.dataset_ref == dataset_id,
                    DATASET_LOCATION.c.uri_scheme == scheme,
                    DATASET_LOCATION.c.uri_body == body,
                    DATASET_LOCATION.c.archived != None,
                )
            ).values(
                archived=None
            )
        )
        return res.rowcount > 0

    def __repr__(self):
        return "PostgresDb<connection={!r}>".format(self._connection)

    def list_users(self):
        result = self._connection.execute("""
            select
                group_role.rolname as role_name,
                user_role.rolname as user_name,
                pg_catalog.shobj_description(user_role.oid, 'pg_authid') as description
            from pg_roles group_role
            inner join pg_auth_members am on am.roleid = group_role.oid
            inner join pg_roles user_role on am.member = user_role.oid
            where (group_role.rolname like 'odc_%%') and not (user_role.rolname like 'odc_%%')
            order by group_role.oid asc, user_role.oid asc;
        """)
        for row in result:
            yield _core.from_pg_role(row['role_name']), row['user_name'], row['description']

    def create_user(self, username, password, role, description=None):
        pg_role = _core.to_pg_role(role)
        username = escape_pg_identifier(self._connection, username)
        sql = text('create user {username} password :password in role {role}'.format(username=username, role=pg_role))
        self._connection.execute(sql,
                                 password=password)
        if description:
            sql = text('comment on role {username} is :description'.format(username=username))
            self._connection.execute(sql,
                                     description=description)

    def drop_users(self, users):
        # type: (Iterable[str]) -> None
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
