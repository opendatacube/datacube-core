import yaml
from datetime import datetime
from datacube.model.fields import get_dataset_fields, parse_dataset_field
from datacube.model import Range
import pytest
import toolz
import uuid

from pathlib import Path
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

EO_STACLIKE_METADATA_DOC = yaml.load('''---
name: eo_stac_like_product
description: Stack_datasets
dataset:
  id: [id]
  creation_dt: ['properties', 'odc:processing_datetime']
  label: [title]
  measurements: [measurements]
  grid_spatial: []
  format: ['properties', 'odc:file_format']
  sources: ['lineage']

  search_fields:
    region_code:
      description: Spatial reference code from the provider. In case of Landsat that region is a scene path,row,
                   in case of ingested products on NCI its Albers tile, for Sentinel its MGRS code.
      offset: [properties, 'eo:region_code']

    platform:
      description: Platform code
      offset: [properties, 'eo:platform']

    instrument:
      description: Instrument name
      offset: [properties, 'eo:instrument']

    time:
      description: Acquisition time range
      type: datetime-range
      min_offset:
        - [properties, 'dtr:start_time']
        - [properties, 'datetime']
      max_offset:
        - [properties, 'dtr:end_time']
        - [properties, 'datetime']

    lat:
      description: Latitude range
      type: double-range
      min_offset:
      - ['extent', 'coord', 'll', 'lat']
      max_offset:
      - ['extent', 'coord', 'ur', 'lat']

    lon:
      description: Longitude range
      type: double-range
      min_offset:
      - ['extent', 'coord', 'ul', 'lon']
      max_offset:
      - ['extent', 'coord', 'lr', 'lon']
''', Loader=Loader)

SAMPLE_ARD_YAML_DOC = yaml.load('''---
# Dataset
$schema: https://schemas.opendatacube.org/dataset

id: a77664c9-02bc-4792-9ec3-62a1e1bfb611
product:
  name: ga_ls5t_ard_3
  href: https://collections.dea.ga.gov.au/product/ga_ls5t_ard_3

crs: epsg:32655
geometry:
  type: Polygon
  coordinates: [[[542488.7966842888, -3723585.0], [542265.9092490782, -3723997.6700470117],
      [539385.8876777751, -3735427.7561961184], [529875.8780597479, -3773647.794960378],
      [502935.87685936113, -3882547.799813938], [498735.85724788456, -3899737.879606813],
      [498585.0584226455, -3900733.1286514155], [498585.0, -3900735.8728160136], [
        498585.0, -3902150.914171281], [504639.3866367633, -3903344.470156993], [
        692175.0, -3938985.0], [693014.1183756727, -3935752.219431985], [735404.1186963692,
        -3764752.2181383856], [736124.3454253719, -3761361.2326567164], [736086.2132034355,
        -3760883.7867965642], [732860.7006571969, -3760215.5466044825], [542660.7715732208,
        -3723585.0], [542488.7966842888, -3723585.0]]]
grids:
  default:
    shape: [7191, 7981]
    transform: [30.0, 0.0, 498585.0, 0.0, -30.0, -3723585.0, 0.0, 0.0, 1.0]

properties:
  datetime: 2017-09-07 23:18:52.566031Z  # Center datetime between dtr:start_time and dtr:end_time (if
                                         # available) else datetime is dataset processed/created datetime
  dea:dataset_maturity: final
  dea:processing_level: level-2
  eo:cloud_cover: 78.0
  eo:gsd: 30.0
  eo:instrument: MSI
  eo:platform: SENTINEL_2B
  eo:sun_azimuth: 42.22906228
  eo:sun_elevation: 19.66008769
  eo:region_code: 55LFC
  landsat:collection_category: T1
  landsat:collection_number: 1
  landsat:landsat_product_id: LT05_L1TP_091084_19930707_20170118_01_T1
  landsat:landsat_scene_id: LT50910841993188ASA00
  landsat:wrs_path: 91
  landsat:wrs_row: 84
  odc:processing_datetime: 2019-03-26 01:51:35.951680Z  # When the dataset was processed/created
  odc:product_family: ard
  odc:reference_code: 091084
  odc:file_format: GeoTIFF
  dtr:start_time: 2017-08-17 00:27:09
  dtr:end_time: 2018-1-1 01:20:39

extent:
    coord:
        ul:
            lat: -31.630651380897287
            lon: 136.05446915038226
        ll:
            lat: -32.621056589489115
            lon: 136.06592304412072
        ur:
            lat: -31.615847630855164
            lon: 137.21173336823486
        lr:
            lat: -32.60567685163435
            lon: 137.23573900049476

measurements:
  nbar_band01:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band01.tif
  nbar_band02:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band02.tif
  nbar_band03:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band03.tif
  nbar_band04:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band04.tif
  nbar_band05:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band05.tif
  nbar_band07:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_nbar-band07.tif
  satellite_view:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_satellite-view.tif
  satellite_azimuth:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_satellite-azimuth.tif
  solar_zenith:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_solar-zenith.tif
  solar_azimuth:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_solar-azimuth.tif
  relative_azimuth:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_relative-azimuth.tif
  timedelta:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_timedelta.tif
  incident_angle:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_incident-angle.tif
  azimuthal_incident:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_azimuthal-incident.tif
  exiting_angle:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_exiting-angle.tif
  azimuthal_exiting:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_azimuthal-exiting.tif
  relative_slope:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_relative-slope.tif
  combined_terrain_shadow:
    path: ga_ls5t_ard_3-0-0_091084_1993-07-07_final_combined-terrain-shadow.tif

lineage:
  level1:
  - b3aa06e9-b8e5-5acc-b29c-2bdfb78ba331
''', Loader=Loader)

EXPECTED_VALUE = dict(
    region_code='55LFC',
    platform='SENTINEL_2B',
    instrument='MSI',
    lat=Range(-32.621056589489115, -31.615847630855164),
    lon=Range(136.05446915038226, 137.23573900049476),
    time=Range(datetime.strptime('2017-08-17 00:27:09', "%Y-%m-%d %H:%M:%S"),
               datetime.strptime('2018-01-01 01:20:39', "%Y-%m-%d %H:%M:%S")),
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


def _crete_sample_product_def(metadata_definition,
                              measurements=('azimuthal_exiting', 'azimuthal_incident', 'combined_terrain_shadow',
                                            'exiting_angle', 'incident_angle', 'nbar_band01', 'nbar_band02',
                                            'nbar_band03', 'nbar_band04', 'nbar_band05', 'nbar_band07',
                                            'relative_azimuth', 'relative_slope', 'satellite_azimuth',
                                            'satellite_view', 'solar_azimuth', 'solar_zenith', 'timedelta'),
                              with_grid_spec=False,
                              storage=None):

    if storage is None and with_grid_spec is True:
        storage = {'crs': 'EPSG:3577',
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


def _create_new_dataset_def(input_file: Path):
    """
    Create new metadata dataset definition yaml files
    """
    now = datetime.utcnow()
    creation_dt = now.strftime('%Y-%m-%dT%H:%M:%S.%f')

    dataset_def_template = {
        'id': str(uuid.uuid4()),
        'creation_dt': creation_dt,
        'label': '',
        'product_type': '',
        'platform': {'code': ''},
        'region': {'code': ''},
        'instrument': {
            'name': ''
        },
        'extent': {
            'coord': {'ll': {'lat': ''},
                      'ur': {'lat': ''},
                      'ul': {'lon': ''},
                      'lr': {'lon': ''}},
        },
        'format': {'name': 'GeoTIFF'},
        'grid_spatial': {},
        'image': {
            'bands': {
                'azimuthal_exiting': {
                    'path': str(input_file.stem) + '_azimuthal-exiting.tif',
                    'layer': 1
                },
                'azimuthal_incident': {
                    'path': str(input_file.stem) + '_azimuthal-incident.tif',
                    'layer': 1
                },
                'combined_terrain_shadow': {
                    'path': str(input_file.stem) + '_combined-terrain-shadow.tif',
                    'layer': 1
                },
                'exiting_angle': {
                    'path': str(input_file.stem) + '_exiting-angle.tif',
                    'layer': 1
                },
                'incident_angle': {
                    'path': str(input_file.stem) + '_incident-angle.tif',
                    'layer': 1
                },
                'nbar_band01': {
                    'path': str(input_file.stem) + '_nbar-band01.tif',
                    'layer': 1
                },
                'nbar_band02': {
                    'path': str(input_file.stem) + '_nbar-band02.tif',
                    'layer': 1
                },
                'nbar_band03': {
                    'path': str(input_file.stem) + '_nbar-band03.tif',
                    'layer': 1
                },
                'nbar_band04': {
                    'path': str(input_file.stem) + '_nbar-band04.tif',
                    'layer': 1
                },
                'nbar_band05': {
                    'path': str(input_file.stem) + '_nbar-band05.tif',
                    'layer': 1
                },
                'nbar_band07': {
                    'path': str(input_file.stem) + '_nbar-band07.tif',
                    'layer': 1
                },
                'relative_azimuth': {
                    'path': str(input_file.stem) + '_relative-azimuth.tif',
                    'layer': 1
                },
                'relative_slope': {
                    'path': str(input_file.stem) + '_relative-slope.tif',
                    'layer': 1
                },
                'satellite_azimuth': {
                    'path': str(input_file.stem) + '_satellite-azimuth.tif',
                    'layer': 1
                },
                'satellite_view': {
                    'path': str(input_file.stem) + '_satellite-view.tif',
                    'layer': 1
                },
                'solar_azimuth': {
                    'path': str(input_file.stem) + '_solar-azimuth.tif',
                    'layer': 1
                },
                'solar_zenith': {
                    'path': str(input_file.stem) + '_solar-zenith.tif',
                    'layer': 1
                },
                'timedelta': {
                    'path': str(input_file.stem) + '_timedelta.tif',
                    'layer': 1
                }
            }
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

    for key, value in ds_search_field.items():
        if key in 'region_code':
            dataset_def_template['region']['code'] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in 'lat':
            min_offset = _convert_datetime(value.extract(SAMPLE_ARD_YAML_DOC).begin)
            max_offset = _convert_datetime(value.extract(SAMPLE_ARD_YAML_DOC).end)
            dataset_def_template['extent']['coord']['ll'][key] = min_offset
            dataset_def_template['extent']['coord']['ur'][key] = max_offset
        elif key in 'lon':
            min_offset = _convert_datetime(value.extract(SAMPLE_ARD_YAML_DOC).begin)
            max_offset = _convert_datetime(value.extract(SAMPLE_ARD_YAML_DOC).end)
            dataset_def_template['extent']['coord']['ul'][key] = min_offset
            dataset_def_template['extent']['coord']['lr'][key] = max_offset
        elif key in 'instrument':
            dataset_def_template[key]['name'] = value.extract(SAMPLE_ARD_YAML_DOC)
        elif key in ('platform', 'region'):
            dataset_def_template[key]['code'] = value.extract(SAMPLE_ARD_YAML_DOC)

    return dataset_def_template


def _create_yamlfile(yaml_path):
    product_definition = _crete_sample_product_def(EO_STACLIKE_METADATA_DOC)
    dataset_definition = _create_new_dataset_def(Path('ga_ls5t_granule_3-0-0_091084_1993-07-07_final'))

    Path(yaml_path).mkdir(parents=True, exist_ok=True)
    product_def_yamlfile = yaml_path + "eo_staclike_new_product.yaml"
    dataset_def_yamlfile = yaml_path + "eo_staclike_new_dataset.yaml"

    with open(product_def_yamlfile, 'w') as fp:
        yaml.dump(product_definition, fp, default_flow_style=False, Dumper=Dumper)
        print(f"Writing product definition to {Path(product_def_yamlfile).name} file")

    with open(dataset_def_yamlfile, 'w') as fp:
        yaml.dump(dataset_definition, fp, default_flow_style=False, Dumper=Dumper)
        print(f"Writing product definition to {Path(dataset_def_yamlfile).name} file")

    return product_def_yamlfile, dataset_def_yamlfile


def test_new_eo_metadata_search_fields():
    ds_search_field = get_dataset_fields(EO_STACLIKE_METADATA_DOC)
    platform = ds_search_field['platform'].extract(SAMPLE_ARD_YAML_DOC)
    assert platform == 'SENTINEL_2B'

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

    clirunner(['dataset', 'add', '--confirm-ignore-lineage', '--product', 'eo_stac_like_product',
               str(dataset_definition)])

    datasets = index.datasets.search_eager(product='eo_stac_like_product')
    assert len(datasets) > 0
    assert not datasets[0].managed

    ds_ = index.datasets.get(_ds['id'], include_sources=True)
    assert ds_ is not None
    assert str(ds_.id) == _ds['id']
    assert ds_.sources == {}
