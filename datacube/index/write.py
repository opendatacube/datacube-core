# coding=utf-8
"""
Methods for writing to the index: datasets, config etc.

Usually only performable by admins or privileged users.
"""
from __future__ import absolute_import

import yaml
from neocommon.serialise import to_simple_type
from sqlalchemy import create_engine

from . import _model


def index_dataset(connection, dataset_d, path=None):
    dataset_id = dataset_d['id']
    source_datsets = dataset_d['lineage']['source_datasets']

    # Clear them. We don't store them.
    dataset_d['lineage']['source_datasets'] = None

    sources = {}

    # Get source datasets, insert them.
    for classifier, source_dataset in source_datsets.items():
        source_id = index_dataset(connection, source_dataset)
        sources[classifier] = source_id

    product_type = dataset_d['product_type']

    # TODO: If throws error, dataset may exist already.
    connection.execute(
        _model.dataset.insert().values(
            id=dataset_id,
            type=product_type,
            # TODO: Does path make sense? Or a separate table?
            metadata_path=path,
            metadata=dataset_d
        )
    )

    # Link to sources.
    for classifier, source_dataset_id in sources.items():
        connection.execute(_model.dataset_source.insert().values(
            classifier=classifier,
            dataset_ref=dataset_id,
            source_dataset_ref=source_dataset_id
        ))

    return dataset_id

# TODOs:
# - Write storage config.
# - Write ingest config.
# Command-line API for all three.
# Database information from config file.


# For development..
def _ingest_one(metadata_path):
    engine = create_engine('postgresql:///agdc', echo=True)
    connection = engine.connect()

    _model.ensure_db(connection, engine)

    parsed_metadata = yaml.load(open(metadata_path))

    index_dataset(connection, to_simple_type(parsed_metadata), path=metadata_path)


if __name__ == '__main__':
    import sys

    _ingest_one(sys.argv[1])
