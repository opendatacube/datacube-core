import yaml
from datetime import datetime
from datacube.model.fields import get_dataset_fields
from datacube.model import Range

EO_STACLIKE_METADATA_DOC = yaml.safe_load('''---
name: stac-like
description: test stack datasets
dataset:
  id: [id]
  creation_dt: ['properties', 'odc:processing_datetime']
  label: ['title']
  measurements: ['measurements']
  grid_spatial: []
  format: ['properties', 'odc:file_format']
  sources: ['lineage']

  search_fields:
    region_code:
      description: Spatial reference code from the provider. In case of Landsat that region is a "scene path,row",
                   in case of ingested products on NCI it's Albers tile, for Sentinel it's MGRS code.
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
''')

SAMPLE_ARD_YAML_DOC = yaml.safe_load('''---
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
''')

EXPECTED_VALUE = dict(
    region_code='55LFC',
    platform='SENTINEL_2B',
    instrument='MSI',
    lat=Range(-32.621056589489115, -31.615847630855164),
    lon=Range(136.05446915038226, 137.23573900049476),
    time=Range(datetime.strptime('2017-08-17 00:27:09', "%Y-%m-%d %H:%M:%S"),
               datetime.strptime('2018-01-01 01:20:39', "%Y-%m-%d %H:%M:%S")),
)


def test_new_eo_metadata_search_fields():
    ds_field = get_dataset_fields(EO_STACLIKE_METADATA_DOC)
    platform = ds_field['platform'].extract(SAMPLE_ARD_YAML_DOC)
    assert platform == 'SENTINEL_2B'

    for key, value in ds_field.items():
        assert key == value.name
        assert isinstance(value.description, str)

        res = value.extract(SAMPLE_ARD_YAML_DOC)
        print(res)
        assert res == EXPECTED_VALUE[key]

        # Missing data should return None
        assert value.extract({}) is None
