"""S3 indexing module."""
from __future__ import absolute_import

import logging
from uuid import uuid4

import numpy as np
from sqlalchemy import MetaData
from sqlalchemy import select, and_

import datacube.index._datasets as base_dataset
from datacube.index._api import Index
from datacube.drivers.s3block_index._schema import S3_DATASET, S3_DATASET_CHUNK, S3_DATASET_MAPPING
from datacube.index.postgres.tables import _pg_exists
from datacube.index.postgres.tables._core import SQL_NAMING_CONVENTIONS, SCHEMA_NAME

_LOG = logging.getLogger(__name__)

S3_METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)


class S3DatabaseException(Exception):
    """Raised on errors to do with the S3 block specific database"""


class S3BlockIndex(Index):
    """The s3 indexer extends the existing postgres indexer functionality
    by writing additional s3 information to specific tables.
    """

    def __init__(self, index=None, uri_scheme='s3+block', *args, **kargs):
        """Initialise the index and its dataset resource."""
        super(S3BlockIndex, self).__init__(index, *args, **kargs)
        if not self.connected_to_s3_database():
            raise S3DatabaseException('Not connected to an S3 Database')
        self.datasets = DatasetResource(self._db, self.products, uri_scheme)

    def connected_to_s3_database(self):
        """Check requirements are satisfied.

        :return: True if requirements is satisfied, otherwise returns False
        """
        # check database
        # pylint: disable=protected-access
        try:
            with self._db.connect() as connection:
                return (_pg_exists(connection._connection, "agdc.s3_dataset") and
                        _pg_exists(connection._connection, "agdc.s3_dataset_chunk") and
                        _pg_exists(connection._connection, "agdc.s3_dataset_mapping"))
        except AttributeError:
            _LOG.warning('Should only be here for tests.')
            return True

    def init_db(self, with_default_types=True, with_permissions=True):
        is_new = super(S3BlockIndex, self).init_db(with_default_types, with_permissions)

        if is_new:
            with self._db.connect() as c:
                try:
                    c.execute('begin')
                    _LOG.info('Creating s3 block tables.')

                    S3_METADATA.create_all(c)
                    c.execute('commit')
                except:
                    c.execute('rollback')
                    raise

        return is_new

    def add_datasets(self, datasets, sources_policy='verify'):
        """Index several datasets using the current driver.

        Perform the normal indexing, followed by the s3 specific
        indexing. If the normal indexing fails for any dataset, then
        no s3 indexing takes place and a `ValueError` is raised.

        :param datasets: The datasets to be indexed. It must contain
          an attribute named `storage_output` otherwise a ValueError
          is raised.
        :param str sources_policy: The sources policy.
        :return: The number of datasets indexed.
        :rtype: int

        """
        if 'storage_output' not in datasets.attrs:
            raise ValueError('s3 storage output not received, indexing aborted.')
        dataset_refs = []
        n = 0
        for dataset in datasets.values:
            self.datasets.add(dataset, sources_policy=sources_policy)
            dataset_refs.append(dataset.id)
            n += 1
        if n == len(datasets):
            self.datasets.add_s3_tables(dataset_refs, datasets.attrs['storage_output'])
        else:
            raise ValueError('Some datasets could not be indexed, hence no s3 indexing will happen.')
        return n

    def __repr__(self):
        return "S3Index<db={!r}>".format(self._db)


class DatasetResource(base_dataset.DatasetResource):
    """The s3 dataset resource extends the postgres one by writing
    additional s3 information to specific tables.
    """

    def __init__(self, db, dataset_type_resource, uri_scheme='s3+block'):
        """Initialise the data resource."""
        super(DatasetResource, self).__init__(db, dataset_type_resource)
        self.uri_scheme = uri_scheme

    def add(self, dataset, sources_policy='verify', **kwargs):
        # Set uri scheme to s3
        if dataset.uris:
            dataset.uris = ['%s:%s' % (self.uri_scheme, uri.split(':', 1)[1])
                            for uri in dataset.uris]
        else:
            dataset.uris = []
        return super(DatasetResource, self).add(dataset, sources_policy, **kwargs)

    def _add_s3_dataset(self, transaction, s3_dataset_id, band, output):
        """Add the new s3 dataset to DB.

        :param transaction: Postgres transaction.
        :param uuid s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param dict output: Dictionary of metadata consigning
          the s3 storage information for that band.
        """
        # Build regular indices as list of triple scalars (as
        # numpy types are not accepted by sqlalchemy)
        regular_indices = [list(map(np.asscalar, index))
                           for index in output['regular_index'] if index is not None]

        # Build irregular indices list, padding them all to
        # the same size as required by Postgres
        # multidimensional arrays
        irregular_indices = [list(map(np.asscalar, index))
                             for index in output['irregular_index'] if index is not None]
        if irregular_indices:
            irregular_max_size = max(map(len, irregular_indices))
            irregular_indices = [index + [None] * (irregular_max_size - len(index))
                                 for index in irregular_indices]

        _LOG.debug('put_s3_dataset(%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s)', s3_dataset_id,
                   output['base_name'], band, output['bucket'],
                   output['macro_shape'], output['chunk_size'],
                   output['numpy_type'], output['dimensions'],
                   output['regular_dims'], regular_indices,
                   irregular_indices)
        self.put_s3_dataset(transaction,
                            s3_dataset_id,
                            output['base_name'],
                            band,
                            output['bucket'],
                            output['macro_shape'],
                            output['chunk_size'],
                            output['numpy_type'],
                            output['dimensions'],
                            output['regular_dims'],
                            regular_indices,
                            irregular_indices)

    def _add_s3_dataset_chunks(self, transaction, s3_dataset_id, band, output):
        """Add details of chunks composing this s3 dataset to DB.

        :param transaction: Postgres transaction.
        :param uuid s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param dict output: Dictionary of metadata consigning
          the s3 storage information for that band.
        """
        for key_map in output['key_maps']:
            micro_shape = [chunk_dim.stop - chunk_dim.start for chunk_dim in key_map['chunk']]
            # Convert index_min and index_max to scalars
            index_min = list(map(np.asscalar, key_map['index_min']))
            index_max = list(map(np.asscalar, key_map['index_max']))

            _LOG.debug('put_s3_dataset_chunk(%s, %s, %s, %s, %s, %s, %s)',
                       s3_dataset_id, key_map['s3_key'],
                       key_map['chunk_id'], key_map['compression'], micro_shape,
                       index_min, index_max)
            self.put_s3_dataset_chunk(transaction,
                                      s3_dataset_id,
                                      key_map['s3_key'],
                                      key_map['chunk_id'],
                                      key_map['compression'],
                                      micro_shape,
                                      index_min,
                                      index_max)

    def _add_s3_dataset_mappings(self, transaction, s3_dataset_id, band, dataset_refs):
        """Add mappings between postgres datsets and s3 datasets to DB.

        :param transaction: Postgres transaction.
        :param UUID s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param list dataset_refs: The list of dataset references
          (uuids) that all point to the s3 dataset entry being
          created.
        """
        for dataset_ref in dataset_refs:
            _LOG.debug('put_s3_mapping(%s, %s, %s)', dataset_ref, band, s3_dataset_id)
            self.put_s3_mapping(transaction,
                                dataset_ref,
                                band,
                                s3_dataset_id)

    def add_s3_tables(self, dataset_refs, storage_output):
        """Add index data to s3 tables.

        :param list dataset_refs: The list of dataset references
          (uuids) that all point to the s3 dataset entry being
          created.
        :param dict storage_output: Dictionary of metadata consigning
          the s3 storage information.
        """
        # Roll back if any exception arise
        with self._db.begin() as transaction:
            for band, output in storage_output.items():
                # Create a random UUID for this s3 dataset/band pair
                s3_dataset_id = uuid4()

                # Add s3 dataset
                self._add_s3_dataset(transaction, s3_dataset_id, band, output)

                # Add chunks
                self._add_s3_dataset_chunks(transaction, s3_dataset_id, band, output)

                # Add mappings
                self._add_s3_dataset_mappings(transaction, s3_dataset_id, band, dataset_refs)

    def _make(self, dataset_res, full_info=False):
        """
        :rtype Dataset

        :param bool full_info: Include all available fields
        """
        dataset = super(DatasetResource, self)._make(dataset_res, full_info)
        self.add_specifics(dataset)
        return dataset

    def add_specifics(self, dataset):
        """Extend the dataset doc with driver specific index data.

        This methods extends the dataset document with a `s3_metadata`
        variable containing the s3 indexing metadata.

        The dataset is modified in place.

        :param :cls:`datacube.model.Dataset` dataset: The dataset to
          add NetCDF-specific indexing data to.
        """
        dataset.s3_metadata = {}
        if dataset.measurements:
            with self._db.begin() as transaction:
                for band in dataset.measurements.keys():
                    s3_datasets = self.get_s3_dataset(transaction, dataset.id, band)
                    for s3_dataset in s3_datasets:
                        dataset.s3_metadata[band] = {
                            's3_dataset': s3_dataset,
                            # TODO(csiro): commenting this out for now, not using it yet.
                            # 's3_chunks': transaction.get_s3_dataset_chunk(s3_dataset.id)
                        }

    # S3 specific functions
    # See .tables for description of each column
    def put_s3_mapping(self, _connection, dataset_ref, band, s3_dataset_id):
        """:type dataset_ref: uuid.UUID
        :type band: str
        :type s3_dataset_id: uuid.UUID"""
        res = _connection.execute(
            S3_DATASET_MAPPING.insert().values(
                id=uuid4(),
                dataset_ref=dataset_ref,
                band=band,
                s3_dataset_id=s3_dataset_id,
            )
        )
        return res.inserted_primary_key[0]

    # pylint: disable=too-many-arguments
    def put_s3_dataset(self, _connection, s3_dataset_id, base_name, band, bucket,
                       macro_shape, chunk_size, numpy_type,
                       dimensions, regular_dims, regular_index,
                       irregular_index):
        """:type s3_dataset_id: uuid.UUID
        :type base_name: str
        :type band: str
        :type bucket: str
        :type macro_shape: array[int]
        :type chunk_size: array[int]
        :type numpy_type: str
        :type dimensions: array[str]
        :type regular_dims: array[bool]
        :type regular_index: array[array[float]]
        :type irregular_index: array[array[float]]"""
        res = _connection.execute(
            S3_DATASET.insert().values(
                id=s3_dataset_id,
                base_name=base_name,
                band=band,
                bucket=bucket,
                macro_shape=macro_shape,
                chunk_size=chunk_size,
                numpy_type=numpy_type,
                dimensions=dimensions,
                regular_dims=regular_dims,
                regular_index=regular_index,
                irregular_index=irregular_index
            )
        )

        return res.inserted_primary_key[0]

    def get_s3_dataset(self, _connection, dataset_ref, band):
        """:type dataset_ref: uuid.UUID
        :type band: str"""
        return _connection.execute(
            select(
                [S3_DATASET.c.id,
                 S3_DATASET.c.base_name,
                 S3_DATASET.c.band,
                 S3_DATASET.c.bucket,
                 S3_DATASET.c.macro_shape,
                 S3_DATASET.c.chunk_size,
                 S3_DATASET.c.numpy_type,
                 S3_DATASET.c.dimensions,
                 S3_DATASET.c.regular_dims,
                 S3_DATASET.c.regular_index,
                 S3_DATASET.c.irregular_index]
            ).select_from(
                S3_DATASET.join(S3_DATASET_MAPPING,
                                S3_DATASET_MAPPING.c.s3_dataset_id == S3_DATASET.c.id,
                                isouter=True)
            ).where(
                and_(
                    S3_DATASET_MAPPING.c.dataset_ref == dataset_ref,
                    S3_DATASET_MAPPING.c.band == band
                )
            )
        ).fetchall()

    def put_s3_dataset_chunk(self, _connection, s3_dataset_id, s3_key,
                             chunk_id, compression_scheme,
                             micro_shape, index_min, index_max):
        """:type s3_dataset_id: uuid.UUID
        :type key: str
        :type chunk_id: str
        :type compression_scheme: str
        :type micro_shape: array[int]
        :type index_min: array[float]
        :type index_max: array[float]"""
        res = _connection.execute(
            S3_DATASET_CHUNK.insert().values(
                id=uuid4(),
                s3_dataset_id=s3_dataset_id,
                s3_key=s3_key,
                chunk_id=chunk_id,
                compression_scheme=compression_scheme,
                micro_shape=micro_shape,
                index_min=index_min,
                index_max=index_max
            )
        )
        return res.inserted_primary_key[0]

    def get_s3_dataset_chunk(self, _connection, s3_dataset_id):
        """:type s3_dataset_id: uuid.UUID"""
        return _connection.execute(
            select(
                [S3_DATASET_CHUNK.c.s3_key,
                 S3_DATASET_CHUNK.c.chunk_id,
                 S3_DATASET_CHUNK.c.compression_scheme,
                 S3_DATASET_CHUNK.c.micro_shape,
                 S3_DATASET_CHUNK.c.index_min,
                 S3_DATASET_CHUNK.c.index_max]
            ).where(
                S3_DATASET_CHUNK.c.s3_dataset_id == s3_dataset_id,
            )
        ).fetchall()
