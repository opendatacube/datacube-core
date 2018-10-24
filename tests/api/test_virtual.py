from datetime import datetime
import uuid

import pytest
import yaml
import mock

from datacube.model import DatasetType, MetadataType, Dataset, GridSpec
from datacube.utils import geometry
from datacube.virtual import construct


def example_metadata_type():
    return MetadataType(dict(name='eo',
                             dataset=dict(id=['id'],
                                          label=['ga_label'],
                                          creation_time=['creation_dt'],
                                          measurements=['image', 'bands'],
                                          grid_spatial=['grid_spatial', 'projection'],
                                          sources=['lineage', 'source_datasets'])),
                        dataset_search_fields={})


def example_product(name):
    result = DatasetType(example_metadata_type(),
                         dict(name=name, description="", metadata_type='eo', metadata={}))
    result.grid_spec = GridSpec(crs=geometry.CRS('EPSG:3577'),
                                tile_size=(100000., 100000.),
                                resolution=(-25, 25))
    return result


def example_grid_spatial():
    return {
        "projection": {
            "valid_data": {
                "type": "Polygon",
                "coordinates": [[[1500000.0, -4000000.0],
                                 [1500000.0, -3900000.0],
                                 [1600000.0, -3900000.0],
                                 [1600000.0, -4000000.0],
                                 [1500000.0, -4000000.0]]]
            },
            "geo_ref_points": {
                "ll": {"x": 1500000.0, "y": -4000000.0},
                "lr": {"x": 1600000.0, "y": -4000000.0},
                "ul": {"x": 1500000.0, "y": -3900000.0},
                "ur": {"x": 1600000.0, "y": -3900000.0}
            },
            "spatial_reference": "EPSG:3577"
        }
    }


@pytest.fixture
def cloud_free_nbar_recipe():
    recipe = yaml.load("""
    collate:
      - transform: apply_mask
        mask_measurement_name: pixelquality
        input:
          &mask
          transform: make_mask
          flags:
              blue_saturated: false
              cloud_acca: no_cloud
              cloud_fmask: no_cloud
              cloud_shadow_acca: no_cloud_shadow
              cloud_shadow_fmask: no_cloud_shadow
              contiguous: true
              green_saturated: false
              nir_saturated: false
              red_saturated: false
              swir1_saturated: false
              swir2_saturated: false
          mask_measurement_name: pixelquality
          input:
            juxtapose:
              - product: ls8_nbar_albers
                measurements: ['blue', 'green']
              - product: ls8_pq_albers
      - transform: datacube.virtual.transformations.ApplyMask
        mask_measurement_name: pixelquality
        input:
          <<: *mask
          input:
            juxtapose:
              - product: ls7_nbar_albers
                measurements: ['blue', 'green']
              - product: ls7_pq_albers
    index_measurement_name: source_index
    """)

    return construct(**recipe)


@pytest.fixture
def product_definitions():
    return {
        'ls7_nbar_albers': {
            'measurements': [dict(name='blue', dtype='int16', nodata=-999, units='1'),
                             dict(name='green', dtype='int16', nodata=-999, units='1')]
        },

        'ls8_nbar_albers': {
            'measurements': [dict(name='blue', dtype='int16', nodata=-999, units='1'),
                             dict(name='green', dtype='int16', nodata=-999, units='1')]
        },

        'ls7_pq_albers': {
            'measurements': [dict(name='pixelquality', dtype='int16', nodata=0, units='1')]
        },

        'ls8_pq_albers': {
            'measurements': [dict(name='pixelquality', dtype='int16', nodata=0, units='1')]
        }
    }


@pytest.fixture
def dc():
    result = mock.MagicMock()
    result.index.datasets.get_field_names.return_value = {'id', 'product', 'time'}

    def example_dataset(product, center_time):
        result = Dataset(example_product(product),
                         dict(id=str(uuid.uuid4()), grid_spatial=example_grid_spatial()),
                         uris=['file://test.zzz'])
        result.center_time = center_time
        return result

    def search(*args, **kwargs):
        product = kwargs['product']
        if product == 'ls8_nbar_albers':
            return [example_dataset(product, datetime(2014, 2, 7, 23, 57, 26)),
                    example_dataset(product, datetime(2014, 1, 1, 23, 57, 26))]
        elif product == 'ls8_pq_albers':
            return [example_dataset(product, datetime(2014, 2, 7, 23, 57, 26))]
        elif product == 'ls7_nbar_albers':
            return [example_dataset(product, datetime(2014, 1, 22, 23, 57, 36))]
        elif product == 'ls7_pq_albers':
            return [example_dataset(product, datetime(2014, 1, 22, 23, 57, 36))]
        else:
            return []

    result.index.products.get_by_name = example_product
    result.index.datasets.search = search
    return result


@pytest.fixture
def query():
    return {
        'time': ('2014-01-01', '2014-03-01'),
        'lat': (-35.2, -35.21),
        'lon': (149.0, 149.01)
    }


def test_name_resolution(cloud_free_nbar_recipe):
    for prod in cloud_free_nbar_recipe['collate']:
        assert callable(prod['transform'])


def test_output_measurements(cloud_free_nbar_recipe, product_definitions):
    measurements = cloud_free_nbar_recipe.output_measurements(product_definitions)
    assert 'blue' in measurements
    assert 'green' in measurements
    assert 'source_index' in measurements
    assert 'pixelquality' not in measurements


def test_group_datasets(cloud_free_nbar_recipe, dc, query):
    query_result = cloud_free_nbar_recipe.query(dc, **query)
    group = cloud_free_nbar_recipe.group(query_result, **query)

    time, _, _ = group.shape
    assert time == 2
