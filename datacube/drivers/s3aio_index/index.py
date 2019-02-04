"""S3 indexing module."""

import logging
from uuid import uuid4

import numpy as np
from sqlalchemy import select, and_

from datacube.drivers.postgres._core import pg_exists
from datacube.drivers.s3aio_index.schema import S3_DATASET, S3_DATASET_CHUNK, S3_DATASET_MAPPING, S3_METADATA
from datacube.index._datasets import DatasetResource as BaseDatasetResource
from datacube.index.index import Index

_LOG = logging.getLogger(__name__)
FORMAT = 'aio'


class S3DatabaseException(Exception):
    """Raised on errors to do with the S3 block specific database"""


class S3AIOIndex(Index):
    """The s3 indexer extends the existing postgres indexer functionality
    by writing additional s3 information to specific tables.
    """

    def __init__(self, db):
        """Initialise the index and its dataset resource."""
        super(S3AIOIndex, self).__init__(db)
        # if not self.connected_to_s3_database():
        #     raise S3DatabaseException('Not connected to an S3 Database')

        self.datasets = DatasetResource(db, self.products)

    def connected_to_s3_database(self):
        """Check we are connected to an appropriately initialised database.

        :return: True if requirements is satisfied, otherwise returns False
        """
        try:
            with self._db.give_me_a_connection() as connection:
                return (pg_exists(connection, "agdc.s3_dataset") and
                        pg_exists(connection, "agdc.s3_dataset_chunk") and
                        pg_exists(connection, "agdc.s3_dataset_mapping"))
        except AttributeError:
            _LOG.warning('Should only be here for tests.')
            return True

    def init_db(self, with_default_types=True, with_permissions=True):
        is_new = super(S3AIOIndex, self).init_db(with_default_types, with_permissions)

        if is_new:
            with self._db.give_me_a_connection() as connection:
                try:
                    connection.execute('begin')
                    _LOG.info('Creating s3 block tables.')

                    S3_METADATA.create_all(connection)
                    connection.execute('commit')
                except Exception:
                    connection.execute('rollback')
                    raise

        return is_new

    def __repr__(self):
        return "S3Index<db={!r}>".format(self._db)


class DatasetResource(BaseDatasetResource):
    """The s3 dataset resource extends the postgres one by writing
    additional s3 information to specific tables.
    """

    def add(self, dataset, with_lineage=None, **kwargs):
        saved_dataset = super(DatasetResource, self).add(dataset, with_lineage=with_lineage, **kwargs)

        if dataset.format == FORMAT:
            storage_metadata = kwargs['storage_metadata']  # It's an error to not include this
            self.add_datasets_to_s3_tables([dataset.id], storage_metadata)
        return saved_dataset

    def add_multiple(self, datasets, with_lineage=None):
        """Index several datasets.

        Perform the normal indexing, followed by the s3 specific
        indexing. If the normal indexing fails for any dataset, then
        no s3 indexing takes place and a `ValueError` is raised.

        :param datasets: The datasets to be indexed. It must contain
          an attribute named `storage_metadata` otherwise a ValueError
          is raised.
        :param bool with_lineage: Whether to recursively add lineage, default: yes
        :return: The number of datasets indexed.
        :rtype: int

        """
        raise SystemError("I don't think this is called or used, it will need to be fixed when "
                          " storage chunks across multiple datasets are used")

        # if 'storage_metadata' not in datasets.attrs:
        #     raise ValueError('s3 storage output not received, indexing aborted.')
        # dataset_refs = []
        # n = 0
        # for dataset in datasets.values:
        #     self.add(dataset, with_lineage=with_lineage)
        #     dataset_refs.append(dataset.id)
        #     n += 1
        # if n == len(datasets):
        #     self.add_datasets_to_s3_tables(dataset_refs, datasets.attrs['storage_metadata'])
        # else:
        #     raise ValueError('Some datasets could not be indexed, hence no s3 indexing will happen.')
        # return n

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
        self._put_s3_dataset(transaction,
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
            self._put_s3_dataset_chunk(transaction,
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
            self._put_s3_mapping(transaction,
                                 dataset_ref,
                                 band,
                                 s3_dataset_id)

    def add_datasets_to_s3_tables(self, dataset_refs, storage_metadata):
        """Add extra dataset metadata to s3 tables.

        :param list dataset_refs: The list of dataset references
          (uuids) that all point to the s3 dataset entry being
          created.
        :param dict storage_metadata: Dictionary of metadata consigning
          the s3 storage information.
        """
        # Roll back if any exception arise
        with self._db.begin() as transaction:
            for band, output in storage_metadata.items():
                # Create a random UUID for this s3 dataset/band pair
                s3_dataset_id = uuid4()

                # Add s3 dataset
                self._add_s3_dataset(transaction, s3_dataset_id, band, output)

                # Add chunks
                self._add_s3_dataset_chunks(transaction, s3_dataset_id, band, output)

                # Add mappings
                self._add_s3_dataset_mappings(transaction, s3_dataset_id, band, dataset_refs)

    def _make(self, dataset_res, full_info=False, product=None):
        """
        :rtype Dataset

        :param bool full_info: Include all available fields
        """
        dataset = super(DatasetResource, self)._make(dataset_res, full_info=full_info, product=product)
        self._extend_dataset_with_s3_metadata(dataset)
        return dataset

    def _extend_dataset_with_s3_metadata(self, dataset):
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
                    s3_datasets = self._get_s3_dataset(transaction, dataset.id, band)
                    for s3_dataset in s3_datasets:
                        dataset.s3_metadata[band] = {
                            's3_dataset': s3_dataset,
                            # TODO(csiro): commenting this out for now, not using it yet.
                            # 's3_chunks': transaction.get_s3_dataset_chunk(s3_dataset.id)
                        }

    ### S3 specific functions
    # See .tables for description of each column
    def _put_s3_mapping(self, _connection, dataset_ref, band, s3_dataset_id):
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
    def _put_s3_dataset(self, _connection, s3_dataset_id, base_name, band, bucket,
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

    def _get_s3_dataset(self, _connection, dataset_ref, band):
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

    def _put_s3_dataset_chunk(self, _connection, s3_dataset_id, s3_key,
                              chunk_id, compression_scheme,
                              micro_shape, index_min, index_max):
        """:type s3_dataset_id: uuid.UUID
        :type s3_key: str
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

    def _get_s3_dataset_chunk(self, _connection, s3_dataset_id):
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
