# coding=utf-8
"""
Fields that can be queried or indexed within a dataset or storage.
"""
from __future__ import absolute_import

# TODO: Store in DB? This doesn't change often, so is hardcoded for now.
DATASET_QUERY_FIELDS = {
    'eo': {
        'lat': {
            'type': 'float-range',
            'min': [
                ['extent', 'coord', 'ul', 'lat'],
                ['extent', 'coord', 'll', 'lat']
            ],
            'max': [
                ['extent', 'coord', 'ur', 'lat'],
                ['extent', 'coord', 'lr', 'lat']
            ]
        },
        'lon': {
            'type': 'float-range',
            'min': [
                ['extent', 'coord', 'll', 'lon'],
                ['extent', 'coord', 'lr', 'lon']
            ],
            'max': [
                ['extent', 'coord', 'ul', 'lon'],
                ['extent', 'coord', 'ur', 'lon']
            ]
        },
        # 't': {
        #     'type': 'datetime-range',
        #     'min': [
        #         ['extent', 'from_dt']
        #     ],
        #     'max': [
        #         ['extent', 'to_dt']
        #     ]
        # },

        # Default to string type.
        'satellite': {
            'offset': ['platform', 'code']
        },
        'sensor': {
            'offset': ['instrument', 'name']
        },
        'gsi': {
            'label': 'Groundstation identifier',
            'offset': ['acquisition', 'groundstation', 'code']
        },
        'sat_path': {
            'type': 'float-range',
            'min': [
                ['image', 'satellite_ref_point_start', 'x']
            ],
            'max': [
                ['image', 'satellite_ref_point_end', 'x']
            ]
        },
        'sat_row': {
            'type': 'float-range',
            'min': [
                ['image', 'satellite_ref_point_start', 'y']
            ],
            'max': [
                ['image', 'satellite_ref_point_end', 'y']
            ]
        }
    }
}
