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
        self.datasets = DatasetResource(self._db, self.products)


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
            # Set uri scheme to s3
            dataset.uris = ['%s:%s' % (self.uri_scheme, uri.split(':', 1)[1]) for uri in dataset.uris] \
                           if dataset.uris else []
            self.datasets.add(dataset, sources_policy='skip')
            dataset_refs.append(dataset.id)
            n += 1
        if n == len(datasets):
            self.datasets.add_s3_tables(dataset_refs, datasets.attrs['storage_output'])
        else:
            raise ValueError('Some datasets could not be indexed, hence no s3 indexing will happen.')
        return n


    def get_specifics(self, dataset):
        s3_datasets = []
        with self._db.begin() as transaction:
            for band in dataset.measurements.keys():
                datasets = transaction.get_s3_dataset(dataset.id, band)
                for dataset in datasets:
                    s3_datasets.append({
                        'metadata': dataset,
                        'chunks': transaction.get_s3_dataset_chunk(dataset.id)
                    })
        return s3_datasets


# pylint: disable=protected-access
class DatasetResource(datacube.index._datasets.DatasetResource):
    '''The s3 dataset resource extends the postgres one by writing
    additional s3 information to specific tables.
    '''

    def __init__(self, db, dataset_type_resource):
        '''Initialise the data resource.'''
        super(DatasetResource, self).__init__(db, dataset_type_resource)
        self.logger = logging.getLogger(self.__class__.__name__)


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

        self.logger.debug('put_s3_dataset(%s, %s, %s, %s, %s, %s, %s, %s ,%s, %s)',
                          s3_dataset_id, output['base_name'], band, output['macro_shape'],
                          output['chunk_size'], output['numpy_type'], output['dimensions'],
                          output['regular_dims'], regular_indices, irregular_indices)
        transaction.put_s3_dataset(s3_dataset_id,
                                   output['base_name'],
                                   band,
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

            self.logger.debug('put_s3_dataset_chunk(%s, %s, %s, %s, %s, %s, %s, %s)',
                              s3_dataset_id, key_map['s3_key'], output['bucket'],
                              key_map['chunk_id'], key_map['compression'], micro_shape,
                              index_min, index_max)
            transaction.put_s3_dataset_chunk(s3_dataset_id,
                                             key_map['s3_key'],
                                             output['bucket'],
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
        """
        :rtype datacube.model.Dataset

        :param bool full_info: Include all available fields
        """
        uris = dataset_res.uris
        if uris:
            uris = [uri for uri in uris if uri] if uris else []
        dataset = Dataset(
            type_=self.types.get(dataset_res.dataset_type_ref),
            metadata_doc=dataset_res.metadata,
            uris=uris,
            indexed_by=dataset_res.added_by if full_info else None,
            indexed_time=dataset_res.added if full_info else None,
            archived_time=dataset_res.archived
        )

        # dataset_res keys: ['id', 'metadata_type_ref', 'dataset_type_ref', 'metadata',
        # 'archived', 'added', 'added_by', 'uri']
        self.logger.debug('@@@@@@@@@@ %s', uris)

        # Pull from s3 tables here?
        with self._db.connect() as connection:
            self.logger.debug('get_s3_mapping(%s, %s)', dataset_res.id, dataset_res.uris)
#            res = connection.get_s3_mapping(dataset_res.id, None)
#            self.logger.debug('@@@@@@@@@@ %s', res)


        # dataset.driver_metadata = {} # for all bands

        return dataset
