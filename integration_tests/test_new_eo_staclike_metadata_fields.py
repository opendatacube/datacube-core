from datetime import datetime
from osgeo import osr
from pathlib import Path
import pytest
import toolz
import uuid
import yaml
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper
from datacube.model.fields import get_dataset_fields, parse_dataset_field
from datacube.model import Range
from datacube.utils.geometry import CRS
from datacube.utils.geometry import Geometry

ROOT_DIR = Path(__file__).parents[0]
ARD_METADATA_FILE = ROOT_DIR / 'sample_ard_3-0-0_odc-metadata.yaml'
SAMPLE_METADATA_TEMPLATE_FILE = ROOT_DIR / 'new-eo-staclike-metadata-type.yaml'

with open(SAMPLE_METADATA_TEMPLATE_FILE) as metadata_doc:
    EO_STACLIKE_METADATA_DOC = yaml.load(metadata_doc, Loader=Loader)

with open(ARD_METADATA_FILE) as ard_metadata_doc:
    SAMPLE_ARD_YAML_DOC = yaml.load(ard_metadata_doc, Loader=Loader)

EXPECTED_VALUE = dict(
    region_code='092084',
    platform='landsat-5',
    instrument='TM',
    lat=None,
    lon=None,
    time=Range(datetime.strptime('2009-12-17 23:53:03.946464', "%Y-%m-%d %H:%M:%S.%f"),
               datetime.strptime('2009-12-17 23:53:31.463325', "%Y-%m-%d %H:%M:%S.%f")),
)


def mk_measurement(m):
    common = dict(dtype='int16',
                  nodata=-999,
                  units='1')

    if isinstance(m, str):
        return dict(name=m, **common)
    elif isinstance(m, tuple):
        name, dtype, nodata = m
        m = common.copy()
        m.update(name=name, dtype=dtype, nodata=nodata)
        return m
    elif isinstance(m, dict):
        m_merged = common.copy()
        m_merged.update(m)
        return m_merged
    else:
        raise ValueError('Only support str|dict|(name, dtype, nodata)')


def _create_sample_product_def(metadata_definition,
                               measurements=('nbar_band01', 'nbar_band02', 'nbar_band03', 'nbar_band04', 'nbar_band05',
                                             'nbar_band07', 'nbart_band01', 'nbart_band02', 'nbart_band03',
                                             'nbart_band04', 'nbart_band05', 'nbart_band07', 'oa_azimuthal_exiting',
                                             'oa_azimuthal_incident', 'oa_combined_terrain_shadow', 'oa_exiting_angle',
                                             'oa_fmask', 'oa_incident_angle', 'oa_nbar_contiguity',
                                             'oa_nbart_contiguity', 'oa_relative_azimuth', 'oa_relative_slope',
                                             'oa_satellite_azimuth', 'oa_satellite_view', 'oa_solar_azimuth',
                                             'oa_solar_zenith', 'oa_time_delta'),
                               with_grid_spec=False,
                               storage=None):

    if storage is None and with_grid_spec is True:
        storage = {'crs': 'epsg:4326',
                   'resolution': {'x': 25, 'y': -25},
                   'tile_size': {'x': 100000.0, 'y': 100000.0}}

    measurements = [mk_measurement(m) for m in measurements]

    definition = dict(
        name=metadata_definition['name'],
        description=metadata_definition['description'],
        metadata_type='eo',
        metadata={},
        measurements=measurements
    )

    if storage is not None:
        definition['storage'] = storage

    return definition


def _get_product_info(metadata_definition):
    fields = toolz.get_in(['dataset'], metadata_definition, {})
    return {name: parse_dataset_field(value, name=name) for name, value in fields.items()
            if name not in ('search_fields', 'grid_spatial')}


def _convert_datetime(val):
    try:
        return val.strftime("%Y-%m-%d %H:%M:%S.%f")
    except AttributeError:
        return str(val)


def _get_geo_boundingbox(metadata_fname):
    with open(metadata_fname) as fl:
        metadata = yaml.load(fl)
    crs = CRS(metadata['crs'])
    geo = Geometry(metadata['geometry'], crs=crs)
    left, bottom, right, top = geo.to_crs(CRS('EPSG:4326')).boundingbox

    spatial_reference = str(crs.wkt)
    geo_ref_points = {
        'ul': {'x': left, 'y': top},
        'll': {'x': left, 'y': bottom},
        'ur': {'x': right, 'y': top},
        'lr': {'x': right, 'y': bottom},
        }
    return geo_ref_points, spatial_reference


def _get_coords(geo_ref_points, spatial_ref):
    spatial_ref = osr.SpatialReference(spatial_ref)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def _transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: _transform(p) for key, p in geo_ref_points.items()}


def _create_new_dataset_def(input_file: Path):
    """
    Create new metadata dataset definition yaml files
    """
    now = datetime.utcnow()
    creation_dt = now.strftime('%Y-%m-%dT%H:%M:%S.%f')

    geo_ref_points, spatial_ref = _get_geo_boundingbox(input_file)

    def _get_measurements(m):
        m_dict = {}
        for band, img_path in m.items():
            m_dict[band] = {'path': str(img_path['path']), 'layer': 1}
        return m_dict

    dataset_def_template = {
        'id': str(uuid.uuid4()),
        'creation_dt': creation_dt,
        'label': '',
        'product_type': 'ard',
        'platform': {'code': ''},
        'region': {'code': ''},
        'instrument': {
            'name': ''
        },
        'extent': {
            'coord': _get_coords(geo_ref_points, spatial_ref),
        },
        'format': {'name': 'GeoTIFF'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': spatial_ref,
            }
        },
        'image': {
            'bands': {}
        },
        'lineage': {
            'source_datasets': {}
        }
    }
    ds_field = _get_product_info(EO_STACLIKE_METADATA_DOC)
    ds_search_field = get_dataset_fields(EO_STACLIKE_METADATA_DOC)

    for key, value in ds_field.items():
        if key not in ('id', 'creation_dt', 'measurements', 'format', 'sources'):
            dataset_def_template[key] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in 'sources':
            dataset_def_template['lineage']['source_datasets'] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in 'measurements':
            measurements = _get_measurements(value.extract(SAMPLE_ARD_YAML_DOC))
            dataset_def_template['image']['bands'] = measurements

    for key, value in ds_search_field.items():
        if key in 'region_code':
            dataset_def_template['region']['code'] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in 'instrument':
            dataset_def_template[key]['name'] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in ('platform', 'region'):
            dataset_def_template[key]['code'] = value.extract(SAMPLE_ARD_YAML_DOC)

    return dataset_def_template


def _create_yamlfile(yaml_path):
    product_definition = _create_sample_product_def(EO_STACLIKE_METADATA_DOC)
    dataset_definition = _create_new_dataset_def(ARD_METADATA_FILE)

    Path(yaml_path).mkdir(parents=True, exist_ok=True)
    product_def_yamlfile = yaml_path + "eo_staclike_new_product.yaml"
    dataset_def_yamlfile = yaml_path + "eo_staclike_new_dataset.yaml"

    with open(product_def_yamlfile, 'w') as fp:
        yaml.dump(product_definition, fp, default_flow_style=False, Dumper=Dumper)

    with open(dataset_def_yamlfile, 'w') as fp:
        yaml.dump(dataset_definition, fp, default_flow_style=False, Dumper=Dumper)

    return product_def_yamlfile, dataset_def_yamlfile


def test_new_eo_metadata_search_fields():
    ds_search_field = get_dataset_fields(EO_STACLIKE_METADATA_DOC)
    platform = ds_search_field['platform'].extract(SAMPLE_ARD_YAML_DOC)
    assert platform == 'landsat-5'

    for key, value in ds_search_field.items():
        assert key == value.name
        assert isinstance(value.description, str)

        res = value.extract(SAMPLE_ARD_YAML_DOC)
        assert res == EXPECTED_VALUE[key]

        # Missing data should return None
        assert value.extract({}) is None


@pytest.mark.usefixtures('ga_metadata_type_doc')
@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
def test_index_new_product(clirunner, index, tmpdir, ga_metadata_type_doc, datacube_env_name):
    """
    The index product with new metadata changes
    """
    product_definition, dataset_definition = _create_yamlfile(str(tmpdir))
    with open(dataset_definition) as config_file:
        _ds = yaml.load(config_file, Loader=Loader)

    # Add the new metadata file
    clirunner(['-v', 'product', 'add', product_definition])

    clirunner(['dataset', 'add', '--confirm-ignore-lineage', '--product', 'ga_ls5t_ard_3',
               str(dataset_definition)])

    datasets = index.datasets.search_eager(product='ga_ls5t_ard_3')
    assert len(datasets) > 0
    assert not datasets[0].managed

    ds_ = index.datasets.get(_ds['id'], include_sources=True)
    assert ds_ is not None
    assert str(ds_.id) == _ds['id']
    assert ds_.sources == {}
