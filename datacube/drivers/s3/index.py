'''S3 indexing module.'''
from __future__ import absolute_import

import logging
from uuid import uuid4
from datetime import datetime
import numpy as np

from datacube.config import LocalConfig
import datacube.index._api
from datacube.index.postgres import PostgresDb
from datacube.model import Dataset
from datacube.drivers.manager import DriverManager

# pylint: disable=protected-access
class Index(datacube.index._api.Index):
    '''The s3 indexer extends the existing postgres indexer functionality
    by writing additional s3 information to specific tables.
    '''

    def __init__(self, local_config=None, application_name=None, validate_connection=True, db=None, uri_scheme='s3'):
        '''Initialise the index and its dataset resource.'''
        if db is None:
            if local_config is None:
                local_config = LocalConfig.find()
            db = PostgresDb.from_config(local_config,
                                        application_name=application_name,
                                        validate_connection=validate_connection)
        super(Index, self).__init__(db)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri_scheme = uri_scheme
        self.datasets = DatasetResource(self._db, self.products, self.uri_scheme)


    def add_datasets(self, datasets, sources_policy='verify'):
        '''Index several datasets using the current driver.

        Perform the normal indexing, followed by the s3 specific
        indexing. If the normal indexing fails for any dataset, then
        no s3 indexing takes place and a `ValueError` is raised.

        :param datasets: The datasets to be indexed. It must contain
          an attribute named `storage_output` otherwise a ValueError
          is raised.
        :param str sources_policy: The sources policy.
        :return: The number of datasets indexed.
        :rtype: int

        '''
        if not 'storage_output' in datasets.attrs:
            raise ValueError('s3 storage output not received, indexing aborted.')
        dataset_refs = []
        n = 0
        for dataset in datasets.values:
            self.datasets.add(dataset, sources_policy='skip')
            dataset_refs.append(dataset.id)
            n += 1
        if n == len(datasets):
            self.datasets.add_s3_tables(dataset_refs, datasets.attrs['storage_output'])
        else:
            raise ValueError('Some datasets could not be indexed, hence no s3 indexing will happen.')
        return n


    def __repr__(self):
        return "S3Index<db={!r}>".format(self._db)


# pylint: disable=protected-access
class DatasetResource(datacube.index._datasets.DatasetResource):
    '''The s3 dataset resource extends the postgres one by writing
    additional s3 information to specific tables.
    '''

    def __init__(self, db, dataset_type_resource, uri_scheme='s3'):
        '''Initialise the data resource.'''
        super(DatasetResource, self).__init__(db, dataset_type_resource)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri_scheme = uri_scheme


    def add(self, dataset, *args, **kargs):
        # Set uri scheme to s3
        dataset.uris = ['%s:%s' % (self.uri_scheme, uri.split(':', 1)[1]) for uri in dataset.uris] \
                       if dataset.uris else []
        return super(DatasetResource, self).add(dataset, *args, **kargs)


    def _add_s3_dataset(self, transaction, s3_dataset_id, band, output):
        '''Add the new s3 dataset to DB.

        :param transaction: Postgres transaction.
        :param uuid s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param dict output: Dictionary of metadata consigning
          the s3 storage information for that band.
        '''
        # Build regular indices as list of triple scalars (as
        # numpy types are not accepted by sqlalchemy)
        regular_indices = [list(map(np.asscalar, index)) \
                           for index in output['regular_index'] if index is not None]

        # Build irregular indices list, padding them all to
        # the same size as required by Postgres
        # multidimensional arrays
        irregular_indices = [list(map(np.asscalar, index)) \
                             for index in output['irregular_index'] if index is not None]
        if irregular_indices:
            irregular_max_size = max(map(len, irregular_indices))
            irregular_indices = [index + [None] * (irregular_max_size - len(index)) \
                                 for index in irregular_indices]

        self.logger.debug('put_s3_dataset(%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s)', s3_dataset_id,
                          output['base_name'], band, output['bucket'],
                          output['macro_shape'], output['chunk_size'],
                          output['numpy_type'], output['dimensions'],
                          output['regular_dims'], regular_indices,
                          irregular_indices)
        transaction.put_s3_dataset(s3_dataset_id,
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
        '''Add details of chunks composing this s3 dataset to DB.

        :param transaction: Postgres transaction.
        :param uuid s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param dict output: Dictionary of metadata consigning
          the s3 storage information for that band.
        '''
        for key_map in output['key_maps']:
            micro_shape = [chunk_dim.stop - chunk_dim.start for chunk_dim in key_map['chunk']]
            # Convert index_min and index_max to scalars
            index_min = list(map(np.asscalar, key_map['index_min']))
            index_max = list(map(np.asscalar, key_map['index_max']))

            self.logger.debug('put_s3_dataset_chunk(%s, %s, %s, %s, %s, %s, %s)',
                              s3_dataset_id, key_map['s3_key'],
                              key_map['chunk_id'], key_map['compression'], micro_shape,
                              index_min, index_max)
            transaction.put_s3_dataset_chunk(s3_dataset_id,
                                             key_map['s3_key'],
                                             key_map['chunk_id'],
                                             key_map['compression'],
                                             micro_shape,
                                             index_min,
                                             index_max)


    def _add_s3_dataset_mappings(self, transaction, s3_dataset_id, band, dataset_refs):
        '''Add mappings between postgres datsets and s3 datasets to DB.

        :param transaction: Postgres transaction.
        :param uuid s3_dataset_id: The uuid of the s3 dataset.
        :param str band: The band to index this set for.
        :param list dataset_refs: The list of dataset references
          (uuids) that all point to the s3 dataset entry being
          created.
        '''
        for dataset_ref in dataset_refs:
            self.logger.debug('put_s3_mapping(%s, %s, %s)', dataset_ref, band, s3_dataset_id)
            transaction.put_s3_mapping(dataset_ref,
                                       band,
                                       s3_dataset_id)


    def add_s3_tables(self, dataset_refs, storage_output):
        '''Add index data to s3 tables.

        :param list dataset_refs: The list of dataset references
          (uuids) that all point to the s3 dataset entry being
          created.
        :param dict storage_output: Dictionary of metadata consigning
          the s3 storage information.
        '''
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
        '''Pull index data from the DB for a dataset.

        This methods returns a dataset with its document along with a
        `s3_metadata` variable containing the s3 indexing metadata.

        :param dataset_ref: See :meth:`datacube.index._datasets.DatasetResource._make`
        :param full_info: See :meth:`datacube.index._datasets.DatasetResource._make`
        '''
        dataset = super(DatasetResource, self)._make(dataset_res, full_info)

        dataset.s3_metadata = {}
        if dataset.measurements:
            with self._db.begin() as transaction:
                for band in dataset.measurements.keys():
                    s3_datasets = transaction.get_s3_dataset(dataset.id, band)
                    for s3_dataset in s3_datasets:
                        dataset.s3_metadata[band] = {
                            's3_dataset': s3_dataset,
                            's3_chunks': transaction.get_s3_dataset_chunk(s3_dataset.id)
                        }
        return dataset
