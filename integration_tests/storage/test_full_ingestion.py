from __future__ import absolute_import

import netCDF4

from datacube.ingest import index_datasets, store_datasets


sample_mapping = {
    'driver': 'NetCDF CF',
    'match': {'metadata': {'instrument': {'name': 'TM'},
                           'platform': {'code': 'LANDSAT_5'},
                           'product_type': 'EODS_NBAR'}},
    'name': 'LS5 NBAR',
    'storage': [
        {
            'global_attributes': {
                'license': 'Creative Commons Attribution 4.0 International CC BY 4.0',
                'product_version': '0.0.0',
                'source': 'This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/',
                'summary': 'These files are experimental, short lived, and the format will change.',
                'title': 'Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE'},
            'location_name': 'testdata',
            'file_path_template': '{platform[code]}_{instrument[name]}_{lons[0]}_{lats[0]}_'
                                  '{extent[center_dt]:%Y-%m-%dT%H-%M-%S.%f}.nc',
            'measurements': {
                '10': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_10'},
                '20': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_20'},
                '30': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_30'}},
            'name': '1deg_tiles'}]}

sample_storage_type = {
    'chunking': {'t': 1, 'x': 500, 'y': 500},
    'dimension_order': ['t', 'y', 'x'],
    'driver': 'NetCDF CF',
    'name': '1deg_tiles',
    'projection': {
        'spatial_ref':
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
            """},
    'resolution': {'x': 0.00025, 'y': -0.00025},
    'tile_size': {'x': 1.0, 'y': -1.0}}

albers_storage_type = {
    'chunking': {'t': 1, 'x': 500, 'y': 500},
    'dimension_order': ['t', 'y', 'x'],
    'driver': 'NetCDF CF',
    'name': '100km_tiles',
    'projection': {
        'spatial_ref':
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
                    AXIS["Northing",NORTH]]"""
    },
    'resolution': {'x': 25, 'y': -25},
    'tile_size': {'x': 100000, 'y': -100000}}

albers_mapping = {
    'driver': 'NetCDF CF',
    'match': {'metadata': {'instrument': {'name': 'TM'},
                           'platform': {'code': 'LANDSAT_5'},
                           'product_type': 'EODS_NBAR'}},
    'name': 'LS5 NBAR Albers',
    'storage': [
        {
            'global_attributes': {
                'license': 'Creative Commons Attribution 4.0 International CC BY 4.0',
                'product_version': '0.0.0',
                'source': 'This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/',
                'summary': 'These files are experimental, short lived, and the format will change.',
                'title': 'Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE'},
            'location_name': 'testdata',
            'file_path_template': '{platform[code]}_{instrument[name]}_{xs[0]}_{ys[0]}_'
                                  '{extent[center_dt]:%Y-%m-%dT%H-%M-%S.%f}.nc',
            'measurements': {
                '10': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_10'},
                '20': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_20'},
                '30': {'dtype': 'int16',
                       'nodata': -999,
                       'resampling_method': 'cubic',
                       'varname': 'band_30'}},
            'name': '100km_tiles'}]}


def test_full_ingestion(index, default_collection, example_ls5_dataset):
    """

    :param db:
    :return:
    """
    # Load a storage config
    index.storage_types.add(sample_storage_type)
    index.storage_types.add(albers_storage_type)

    # Load a mapping config
    index.mappings.add(sample_mapping)
    index.mappings.add(albers_mapping)

    # Run Ingest on a dataset
    datasets = index_datasets(example_ls5_dataset, index)
    store_datasets(datasets, index)

    # Check dataset is indexed
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == 'bbf3e21c-82b0-11e5-9ba1-a0000100fe80'

    # Check storage units are indexed and written
    sus = index.storage.search_eager()

    latlon = [su for su in sus if su.storage_mapping.name == sample_mapping['name']]
    assert len(latlon) == 12
    with netCDF4.Dataset(latlon[0].filepath) as nco:
        assert nco.variables['band_10'].shape == (1, 4000, 4000)

    albers = [su for su in sus if su.storage_mapping.name == albers_mapping['name']]
    assert len(albers) == 16
    with netCDF4.Dataset(albers[0].filepath) as nco:
        assert nco.variables['band_10'].shape == (1, 4000, 4000)
