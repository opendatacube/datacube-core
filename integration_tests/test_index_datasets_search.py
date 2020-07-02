import pytest
from pathlib import PurePosixPath

from integration_tests.test_full_ingestion import ensure_datasets_are_indexed


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_index_datasets_search_light(index, tmpdir, clirunner,
                                     example_ls5_dataset_paths):
    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    def index_products():
        valid_uuids = []
        for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
            valid_uuids.append(uuid)
            index_dataset(ls5_dataset_path)

        # Ensure that datasets are actually indexed
        ensure_datasets_are_indexed(index, valid_uuids)

        return valid_uuids

    valid_uuids = index_products()

    # Test derived properties such as 'extent'
    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'extent', 'time'),
                                                                  product='ls5_nbar_scene'))
    for dataset in results:
        assert dataset.id in valid_uuids
        # Assume projection is defined as
        #         datum: GDA94
        #         ellipsoid: GRS80
        #         zone: -55
        # for all datasets. This should give us epsg 28355
        assert dataset.extent.crs.epsg == 28355

    # test custom fields
    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene'))
    for dataset in results:
        assert dataset.zone == -55

    # Test conditional queries involving custom fields
    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene',
                                                                  zone='-55'))
    assert len(results) > 0

    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene',
                                                                  zone='-65'))
    assert len(results) == 0

    # Test uris

    # Test datasets with just one uri location
    results_no_uri = list(index.datasets.search_returning_datasets_light(field_names=('id'),
                                                                         product='ls5_nbar_scene'))
    results_with_uri = list(index.datasets.search_returning_datasets_light(field_names=('id', 'uris'),
                                                                           product='ls5_nbar_scene'))
    assert len(results_no_uri) == len(results_with_uri)
    for result in results_with_uri:
        assert len(result.uris) == 1

    # 'uri' field bahave same as 'uris' ('uri' could be deprecated!)
    results_with_uri = list(index.datasets.search_returning_datasets_light(field_names=('id', 'uri'),
                                                                           product='ls5_nbar_scene'))
    assert len(results_no_uri) == len(results_with_uri)
    for result in results_with_uri:
        assert len(result.uri) == 1

    # Add a new uri to a dataset
    new_loc = PurePosixPath(tmpdir.strpath) / 'temp_location' / 'agdc-metadata.yaml'
    index.datasets.add_location(valid_uuids[0], new_loc.as_uri())

    results_with_uri = list(index.datasets.search_returning_datasets_light(field_names=('id', 'uris'),
                                                                           product='ls5_nbar_scene',
                                                                           id=valid_uuids[0]))
    assert len(results_with_uri) == 1
    assert len(results_with_uri[0].uris) == 2

    results_with_uri = list(index.datasets.search_returning_datasets_light(field_names=('id', 'uri'),
                                                                           product='ls5_nbar_scene',
                                                                           id=valid_uuids[0]))
    assert len(results_with_uri) == 1
    assert len(results_with_uri[0].uri) == 2


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_index_get_product_time_bounds(index, clirunner, example_ls5_dataset_paths):
    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    def index_products():
        valid_uuids = []
        for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
            valid_uuids.append(uuid)
            index_dataset(ls5_dataset_path)

        # Ensure that datasets are actually indexed
        ensure_datasets_are_indexed(index, valid_uuids)

        return valid_uuids

    valid_uuids = index_products()

    # lets get time values
    dataset_times = list(index.datasets.search_returning_datasets_light(field_names=('time',),
                                                                        product='ls5_nbar_scene'))

    # get time bounds
    time_bounds = index.datasets.get_product_time_bounds(product='ls5_nbar_scene')
    left = sorted(dataset_times, key=lambda dataset: dataset.time.lower)[0].time.lower
    right = sorted(dataset_times, key=lambda dataset: dataset.time.upper)[-1].time.upper

    assert left == time_bounds[0]
    assert right == time_bounds[1]
