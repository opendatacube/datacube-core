# coding=utf-8
"""
Core classes used across modules.
"""
from __future__ import absolute_import

import yaml


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
    def __init__(self, metadata_type, metadata_doc, metadata_path, path):
        """
        A dataset on disk.

        :param metadata_type: Type of metadata doc (only 'eo' currently supported, format as produced by eodatasets)
        :type metadata_type: str
        :param metadata_doc: the document (typically a parsed json/yaml)
        :type metadata_doc: dict
        :param metadata_path:
        :type metadata_path: Path
        :param path: Path provided for the dataset by the user
            It is not normalised: the metadata path is more useful as a location identifier.
        """
        super(Dataset, self).__init__()
        self.path = path
        self.metadata_type = metadata_type
        self.metadata_doc = metadata_doc
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

        return Dataset(metadata_type, metadata_doc, metadata_path, path)
