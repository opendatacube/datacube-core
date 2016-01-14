from __future__ import absolute_import

import six

import netCDF4
from click.testing import CliRunner

import datacube.scripts.run_ingest

geog_mapping = {
    'driver': 'NetCDF CF',
    'match': {'metadata': {'instrument': {'name': 'TM'},
                           'platform': {'code': 'LANDSAT_5'},
                           'product_type': 'EODS_NBAR'}},
    'name': 'ls5_nbar',

    'global_attributes': {
        'license': 'Creative Commons Attribution 4.0 International CC BY 4.0',
        'product_version': '0.0.0',
        'source': 'This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/',
        'summary': 'These files are experimental, short lived, and the format will change.',
        'title': 'Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE'},
    'location_name': 'testdata',
    'file_path_template': '{platform[code]}_{instrument[name]}_{tile_index[0]}_{tile_index[1]}_'
                          '{start_time:%Y-%m-%dT%H-%M-%S.%f}.nc',
    'measurements': {
        '10': {'dtype': 'int16',
               'nodata': -999,
               'resampling_method': 'cubic',
               'varname': 'band_10'},
        '20': {'dtype': 'int16',
               'nodata': -999,
               'resampling_method': 'cubic',
               'varname': 'band_20'}},
    'storage': {
        'chunking': {'time': 1, 'longitude': 400, 'latitude': 400},
        'dimension_order': ['time', 'latitude', 'longitude'],
        'driver': 'NetCDF CF',
        'crs':
            """
           GEOGCS["WGS 84",
                DATUM["WGS_1984",
                    SPHEROID["WGS 84",6378137,298.257223563,
                        AUTHORITY["EPSG","7030"]],
                    AUTHORITY["EPSG","6326"]],
                PRIMEM["Greenwich",0,
                    AUTHORITY["EPSG","8901"]],
                UNIT["degree",0.0174532925199433,
                    AUTHORITY["EPSG","9122"]],
                AUTHORITY["EPSG","4326"]]
            """,
        'resolution': {'longitude': 0.0025, 'latitude': -0.0025},
        'tile_size': {'longitude': 1.0, 'latitude': 1.0}
    }
}

albers_mapping = {
    'driver': 'NetCDF CF',
    'match': {'metadata': {'instrument': {'name': 'TM'},
                           'platform': {'code': 'LANDSAT_5'},
                           'product_type': 'EODS_NBAR'}},
    'name': 'ls5_nbar_albers',
    'global_attributes': {
        'license': 'Creative Commons Attribution 4.0 International CC BY 4.0',
        'product_version': '0.0.0',
        'source': 'This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/',
        'summary': 'These files are experimental, short lived, and the format will change.',
        'title': 'Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE'},
    'location_name': 'testdata',
    'file_path_template': '{platform[code]}_{instrument[name]}_{tile_index[0]}_{tile_index[1]}_'
                          '{start_time:%Y-%m-%dT%H-%M-%S.%f}.nc',
    'measurements': {
        '10': {'dtype': 'int16',
               'nodata': -999,
               'resampling_method': 'cubic',
               'varname': 'band_10'},
        '20': {'dtype': 'int16',
               'nodata': -999,
               'resampling_method': 'cubic',
               'varname': 'band_20'}
    },
    'storage': {
        'chunking': {'time': 1, 'y': 400, 'x': 400},
        'dimension_order': ['time', 'y', 'x'],
        'driver': 'NetCDF CF',
        'crs':
            """PROJCS["GDA94 / Australian Albers",
                    GEOGCS["GDA94",
                        DATUM["Geocentric_Datum_of_Australia_1994",
                            SPHEROID["GRS 1980",6378137,298.257222101,
                                AUTHORITY["EPSG","7019"]],
                            TOWGS84[0,0,0,0,0,0,0],
                            AUTHORITY["EPSG","6283"]],
                        PRIMEM["Greenwich",0,
                            AUTHORITY["EPSG","8901"]],
                        UNIT["degree",0.01745329251994328,
                            AUTHORITY["EPSG","9122"]],
                        AUTHORITY["EPSG","4283"]],
                    UNIT["metre",1,
                        AUTHORITY["EPSG","9001"]],
                    PROJECTION["Albers_Conic_Equal_Area"],
                    PARAMETER["standard_parallel_1",-18],
                    PARAMETER["standard_parallel_2",-36],
                    PARAMETER["latitude_of_center",0],
                    PARAMETER["longitude_of_center",132],
                    PARAMETER["false_easting",0],
                    PARAMETER["false_northing",0],
                    AUTHORITY["EPSG","3577"],
                    AXIS["Easting",EAST],
                    AXIS["Northing",NORTH]]""",
        'resolution': {'x': 250, 'y': -250},
        'tile_size': {'x': 100000, 'y': 100000}
    }
}


def test_full_ingestion(global_integration_cli_args, index, default_collection, example_ls5_dataset):
    """
    Loads two storage configuration then ingests a sample Landsat 5 scene

    One storage configuration specifies Australian Albers Equal Area Projection,
    the other is simple latitude/longitude.

    The input dataset should be recorded, two sets of netcdf storage units should be created
    and recorded in the database.
    :param db:
    :return:
    """
    # Load a mapping config
    index.mappings.add(geog_mapping)
    index.mappings.add(albers_mapping)

    # Run Ingest script on a dataset
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            str(example_ls5_dataset),
            '-v', '-v'
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.run_ingest.cli,
        opts
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    # Check dataset is indexed
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == 'bbf3e21c-82b0-11e5-9ba1-a0000100fe80'

    # Check storage units are indexed and written
    sus = index.storage.search_eager()

    latlon = [su for su in sus if su.storage_mapping.name == geog_mapping['name']]
    assert len(latlon) == 12
    albers = [su for su in sus if su.storage_mapping.name == albers_mapping['name']]
    assert len(albers) == 12

    for su_path in (latlon[0], albers[0]):
        with netCDF4.Dataset(su_path) as nco:
            assert nco.variables['band_10'].shape == (1, 400, 400)
            check_cf_compliance(nco)
            check_dataset_metadata_in_su(nco, example_ls5_dataset)


def check_cf_compliance(dataset):
    # At the moment the compliance-checker is only compatible with Python 2
    if not six.PY2:
        return

    from compliance_checker.runner import CheckSuite, ComplianceChecker
    NORMAL_CRITERIA = 2

    cs = CheckSuite()
    cs.load_all_available_checkers()
    score_groups = cs.run(dataset, 'cf')

    groups = ComplianceChecker.stdout_output(cs, score_groups, verbose=1, limit=NORMAL_CRITERIA)
    assert cs.passtree(groups, limit=NORMAL_CRITERIA)


def check_dataset_metadata_in_su(nco, dataset_dir):
    assert len(nco.variables['extra_metadata']) == 0
    nco_metadata = netCDF4.chartostring(nco.variables['extra_metadata'][0])
    with open(dataset_dir.join('agdc-metadata.yaml')) as f:
        orig_metadata = f.read()
    assert nco_metadata == orig_metadata
