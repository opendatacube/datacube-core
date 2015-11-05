# coding=utf-8
"""
Methods for writing to the index: datasets, config etc.

Usually only performable by admins or privileged users.
"""
from __future__ import absolute_import

import yaml
from neocommon.serialise import to_simple_type
from sqlalchemy import create_engine

from . import tables


class Db(object):
    def __init__(self, connection):
        self.connection = connection

    def insert_dataset(self, dataset_doc, dataset_id, path, product_type):
        self.connection.execute(
            tables.dataset.insert().values(
                id=dataset_id,
                type=product_type,
                # TODO: Does path make sense? Or a separate table?
                metadata_path=path,
                metadata=dataset_doc
            )
        )

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        self.connection.execute(tables.dataset_source.insert().values(
            classifier=classifier,
            dataset_ref=dataset_id,
            source_dataset_ref=source_dataset_id
        ))


def index_dataset(db, dataset_doc, path=None):
    """

    :type db: Db
    :type dataset_doc: dict
    :type path: pathlib.Path
    :return:
    """
    # TODO: These lookups will depend on the document type.
    dataset_id = dataset_doc['id']
    source_datsets = dataset_doc['lineage']['source_datasets']
    product_type = dataset_doc['product_type']

    # Clear them. We don't store them.
    dataset_doc['lineage']['source_datasets'] = None

    # Get source datasets & index them.
    sources = {}
    for classifier, source_dataset in source_datsets.items():
        source_id = index_dataset(db, source_dataset)
        sources[classifier] = source_id


    # TODO: If throws error, dataset may exist already.
    db.insert_dataset(dataset_doc, dataset_id, path, product_type)

    # Link to sources.
    for classifier, source_dataset_id in sources.items():
        db.insert_dataset_source(classifier, dataset_id, source_dataset_id)

    return dataset_id


def simple_add_dataset(metadata_path):
    engine = create_engine('postgresql:///agdc', echo=True)
    connection = engine.connect()

    tables.ensure_db(connection, engine)
    parsed_metadata = yaml.load(open(metadata_path))

    index_dataset(Db(connection), to_simple_type(parsed_metadata), path=metadata_path)
