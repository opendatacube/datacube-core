from __future__ import absolute_import

import netCDF4

from datacube.ingest import ingest


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
            'location_offset': '{platform[code]}_{instrument[name]}_{lons[0]}_{lats[0]}_{extent[center_dt]:%Y-%m-%dT%H-%M-%S.%f}.nc',
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


def test_full_ingestion(index, example_ls5_dataset):
    """

    :param db:
    :return:
    """
    # Load a storage config
    index.storage_types.add(sample_storage_type)

    # Load a mapping config
    index.mappings.add(sample_mapping)

    # Run Ingest on a dataset
    ingest(example_ls5_dataset, index)

    # Check dataset is indexed
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == 'bbf3e21c-82b0-11e5-9ba1-a0000100fe80'


    # Check storage units are indexed and written
    sus = index.storage.search_eager()
    assert len(sus) == 12
    with netCDF4.Dataset(sus[0].filepath) as nco:
        assert nco.variables['band_10'].shape == (1, 4000, 4000)
