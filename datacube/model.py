# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import

import logging

import yaml

_LOG = logging.getLogger(__name__)


# Match datasets. (for mapping to storage)
class DatasetMatcher(object):
    def __init__(self, metadata):
        # Match by exact metadata properties (a subset of the metadata doc)
        #: :type: dict
        self.metadata = metadata


class StorageType(object):
    def __init__(self, driver, name, descriptor):
        # Name of the storage driver. 'NetCDF CF', 'GeoTiff' etc.
        #: :type: str
        self.driver = driver

        # Name for this config (specified by users)
        #: :type: str
        self.name = name

        # A definition of the storage (understood by the storage driver)
        #: :type: dict
        self.descriptor = descriptor


class StorageMapping(object):
    def __init__(self, storage_type, name, match, measurements, dataset_measurements_offset):
        # Which datasets to match.
        #: :type: DatasetMatcher
        self.match = match

        #: :type: StorageType
        self.storage_type = storage_type

        # A name for the mapping (specified by users). (unique to the storage type)
        #: :type: str
        self.name = name

        # A dictionary of the measurements to store
        # (key is measurement id, value is a doc understood by the storage driver)
        #: :type: dict
        self.measurements = measurements

        # The offset within a dataset document to find a matching set of measuremnts.
        # (they should have at least a path field in the dataset)
        #: :type: str
        self.dataset_measurements_offset = dataset_measurements_offset


class StorageUnit(object):
    def __init__(self, dataset_ids, storage_type, descriptor, path, id_=None):
        #: :type: int
        self.id_ = id_

        #: :type: list[uuid.UUID]
        self.dataset_ids = dataset_ids

        #: :type: StorageType
        self.storage_type = storage_type

        # A descriptor for this segment. (parameters etc)
        # A 'document' understandable by the storage driver. Properties inside may be queried by users.
        #: :type: dict
        self.descriptor = descriptor

        # Path of the 'storage unit' containing this segment.
        #: :type: pathlib.Path
        self.path = path


# TODO: move this and from_path() to a separate dataset-loader module ...?
def _expected_metadata_path(dataset_path):
    """
    Get the path where we expect a metadata file for this dataset.

    (only supports eo metadata docs at the moment)

    :type dataset_path: Path
    :returns doc_type and path to file.
    :rtype: str, Path
    """

    # - A dataset directory expects file 'ga-metadata.yaml'.
    # - A dataset file expects a sibling file with suffix '.ga-md.yaml'.

    if dataset_path.is_dir():
        return 'eo', dataset_path.joinpath('ga-metadata.yaml')

    if dataset_path.is_file():
        return 'eo', dataset_path.parent.joinpath('{}.ga-md.yaml'.format(dataset_path.name))

    raise ValueError('Unhandled path type for %r' % dataset_path)


class Dataset(object):
    def __init__(self, metadata_type, metadata_doc, metadata_path):
        """
        A dataset on disk.

        :param metadata_type: Type of metadata doc (only 'eo' currently supported, format as produced by eodatasets)
        :type metadata_type: str
        :param metadata_doc: the document (typically a parsed json/yaml)
        :type metadata_doc: dict
        :param metadata_path:
        :type metadata_path: Path
        """
        super(Dataset, self).__init__()
        #: :type: str
        self.metadata_type = metadata_type
        #: :type: dict
        self.metadata_doc = metadata_doc
        #: :type: pathlib.Path
        self.metadata_path = metadata_path

    @property
    def id(self):
        return self.metadata_doc['id']

    @classmethod
    def from_path(cls, path):
        metadata_type, metadata_path = _expected_metadata_path(path)
        if not metadata_path:
            raise ValueError('No supported metadata docs found for dataset {}'.format(path))

        # We only support eo datasets docs at the moment (yaml)
        if metadata_type == 'eo':
            metadata_doc = yaml.load(open(str(metadata_path), 'r'))
        else:
            raise ValueError('Only eo docs are supported at the moment (provided {})'.format(metadata_type))

        return Dataset(metadata_type, metadata_doc, metadata_path)
