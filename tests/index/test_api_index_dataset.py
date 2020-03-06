# coding=utf-8


import datetime
from collections import namedtuple
from contextlib import contextmanager
from copy import deepcopy

import pytest
from uuid import UUID

from datacube.index._datasets import DatasetResource
from datacube.index.exceptions import DuplicateRecordError
from datacube.model import DatasetType, MetadataType, Dataset
from datacube.utils.changes import DocumentMismatchError

_nbar_uuid = UUID('f2f12372-8366-11e5-817e-1040f381a756')
_ortho_uuid = UUID('5cf41d98-eda9-11e4-8a8e-1040f381a756')
_telemetry_uuid = UUID('4ec8fe97-e8b9-11e4-87ff-1040f381a756')

# An NBAR with source datasets. Many fields have been removed to keep it semi-focused to our ingest test.
_EXAMPLE_NBAR = {
    'id': str(_nbar_uuid),
    'product_type': 'nbar_brdf',
    'checksum_path': 'package.sha1',
    'ga_label': 'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126',
    'ga_level': 'P54',
    'size_bytes': 4550,
    'platform': {'code': 'LANDSAT_8'},
    'creation_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373).isoformat(),
    'instrument': {'name': 'OLI_TIRS'},
    'format': {'name': 'GeoTIFF'},
    'extent': {
        'center_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373).isoformat(),
        'coord': {
            'ul': {'lat': -26.37259, 'lon': 116.58914},
            'lr': {'lat': -28.48062, 'lon': 118.96145},
            'ur': {'lat': -26.36025, 'lon': 118.92432},
            'll': {'lat': -28.49412, 'lon': 116.58121}
        }
    },
    'lineage': {
        'machine': {},
        'source_datasets': {
            'ortho': {
                'product_level': 'L1T',
                'product_type': 'ortho',
                'id': str(_ortho_uuid),
                'usgs': {
                    'scene_id': 'LC81120792014026ASA00'
                },
                'extent': {
                    'center_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373).isoformat(),
                    'coord': {
                        'ul': {'lat': -26.37259, 'lon': 116.58914},
                        'lr': {'lat': -28.48062, 'lon': 118.96145},
                        'ur': {'lat': -26.36025, 'lon': 118.92432},
                        'll': {'lat': -28.49412, 'lon': 116.58121}
                    }
                },
                'size_bytes': 1854924494,
                'platform': {
                    'code': 'LANDSAT_8'},
                'creation_dt': datetime.datetime(2015, 4, 7, 0, 58, 8).isoformat(),
                'instrument': {'name': 'OLI_TIRS'},
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126',
                'grid_spatial': {
                    'projection': {
                        'spatial_reference': 'EPSG:28350',
                        'resampling_option': 'CUBIC_CONVOLUTION',
                        'geo_ref_points': {
                            'ul': {'y': 7082987.5, 'x': 459012.5},
                            'lr': {'y': 6847987.5, 'x': 692012.5},
                            'ur': {'y': 7082987.5, 'x': 692012.5},
                            'll': {'y': 6847987.5, 'x': 459012.5}
                        },
                        'orientation': 'NORTH_UP',
                    }
                },
                'acquisition': {
                    'groundstation': {
                        'code': 'ASA',
                        'eods_domain_code': '002',
                        'label': 'Alice Springs'
                    }
                },
                'format': {'name': 'GEOTIFF'},
                'lineage': {
                    'algorithm': {
                        'name': 'LPGS',
                        'parameters': {},
                        'version': '2.4.0'
                    },
                    'machine': {},
                    'source_datasets': {
                        'satellite_telemetry_data': {
                            'product_type': 'satellite_telemetry_data',
                            'checksum_path': 'package.sha1',
                            'id': str(_telemetry_uuid),
                            'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                                        '116_074_20150330T022553Z20150330T022657',

                            'ga_level': 'P00',
                            'size_bytes': 637660782,
                            'platform': {
                                'code': 'LANDSAT_8'},
                            'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4).isoformat(),
                            'instrument': {'name': 'OLI_TIRS'},
                            'format': {
                                'name': 'MD'},
                            'lineage': {
                                'source_datasets': {}
                            }
                        }
                    }
                }
            }
        }
    }
}

_EXAMPLE_METADATA_TYPE = MetadataType(
    {
        'name': 'eo',
        'dataset': dict(
            id=['id'],
            label=['ga_label'],
            creation_time=['creation_dt'],
            measurements=['image', 'bands'],
            sources=['lineage', 'source_datasets']
        )
    },
    dataset_search_fields={}
)

_EXAMPLE_DATASET_TYPE = DatasetType(
    _EXAMPLE_METADATA_TYPE,
    {
        'name': 'eo',
        'description': "",
        'metadata_type': 'eo',
        'metadata': {}
    }
)


def _build_dataset(doc):
    sources = {name: _build_dataset(src) for name, src in doc['lineage']['source_datasets'].items()}
    return Dataset(_EXAMPLE_DATASET_TYPE, doc, uris=['file://test.zzz'], sources=sources)


_EXAMPLE_NBAR_DATASET = _build_dataset(_EXAMPLE_NBAR)

DatasetRecord = namedtuple('DatasetRecord', ['id', 'metadata', 'dataset_type_ref', 'uris',
                                             'added', 'added_by', 'archived'])


class MockIndex(object):
    def __init__(self, db):
        self._db = db


class MockDb(object):
    def __init__(self):
        self.dataset = {}
        self.dataset_source = set()

    @contextmanager
    def begin(self):
        yield self

    @contextmanager
    def connect(self):
        yield self

    def get_dataset(self, id):
        return self.dataset.get(id, None)

    def get_locations(self, dataset):
        return ['file:xxx']

    def datasets_intersection(self, ids):
        return [k for k in ids if k in self.dataset]

    def insert_dataset_location(self, *args, **kwargs):
        return

    def insert_dataset(self, metadata_doc, dataset_id, dataset_type_id):
        # Will we pretend this one was already ingested?
        if dataset_id in self.dataset:
            raise DuplicateRecordError('already ingested')

        self.dataset[dataset_id] = DatasetRecord(dataset_id, deepcopy(metadata_doc), dataset_type_id,
                                                 None, None, None, None)
        return True

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        self.dataset_source.add((classifier, dataset_id, source_dataset_id))


class MockTypesResource(object):
    def __init__(self, type_):
        self.type = type_

    def get(self, *args, **kwargs):
        return self.type

    def get_by_name(self, *args, **kwargs):
        return self.type


def test_index_dataset():
    mock_db = MockDb()
    mock_types = MockTypesResource(_EXAMPLE_DATASET_TYPE)
    datasets = DatasetResource(mock_db, mock_types)
    dataset = datasets.add(_EXAMPLE_NBAR_DATASET)

    ids = {d.id for d in mock_db.dataset.values()}
    assert ids == {_nbar_uuid, _ortho_uuid, _telemetry_uuid}

    # Three datasets (ours and the two embedded source datasets)
    assert len(mock_db.dataset) == 3

    # Our three datasets should be linked together
    # Nbar -> Ortho -> Telemetry
    assert len(mock_db.dataset_source) == 2
    assert mock_db.dataset_source == {
        ('ortho', _nbar_uuid, _ortho_uuid),
        ('satellite_telemetry_data', _ortho_uuid, _telemetry_uuid)
    }

    # Nothing ingested, because we reported the first as already ingested.
    dataset = datasets.add(_EXAMPLE_NBAR_DATASET)
    assert len(mock_db.dataset) == 3
    assert len(mock_db.dataset_source) == 2


def test_index_already_ingested_source_dataset():
    mock_db = MockDb()
    mock_types = MockTypesResource(_EXAMPLE_DATASET_TYPE)
    datasets = DatasetResource(mock_db, mock_types)
    dataset = datasets.add(_EXAMPLE_NBAR_DATASET.sources['ortho'])

    assert len(mock_db.dataset) == 2
    assert len(mock_db.dataset_source) == 1

    dataset = datasets.add(_EXAMPLE_NBAR_DATASET)
    assert len(mock_db.dataset) == 3
    assert len(mock_db.dataset_source) == 2


def test_index_two_levels_already_ingested():
    mock_db = MockDb()
    mock_types = MockTypesResource(_EXAMPLE_DATASET_TYPE)
    datasets = DatasetResource(mock_db, mock_types)
    dataset = datasets.add(_EXAMPLE_NBAR_DATASET.sources['ortho'].sources['satellite_telemetry_data'])

    assert len(mock_db.dataset) == 1
    assert len(mock_db.dataset_source) == 0

    dataset = datasets.add(_EXAMPLE_NBAR_DATASET)
    assert len(mock_db.dataset) == 3
    assert len(mock_db.dataset_source) == 2
