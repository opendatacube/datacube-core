# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime

import pytest

from integration_tests.utils import ensure_datasets_are_indexed


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
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
    dataset_times = list(index.datasets.search_returning(field_names=('time',),
                                                         product='ls5_nbar_scene'))

    # get time bounds
    time_bounds = index.datasets.get_product_time_bounds(product='ls5_nbar_scene')  # Test of deprecated method
    left = sorted(dataset_times, key=lambda dataset: dataset.time.lower)[0].time.lower
    right = sorted(dataset_times, key=lambda dataset: dataset.time.upper)[-1].time.upper

    assert left == time_bounds[0]
    assert right == time_bounds[1]


def test_temporal_extent(
    index, ls8_eo3_dataset, ls8_eo3_dataset2, ls8_eo3_dataset3, ls8_eo3_dataset4
):
    with pytest.raises(KeyError):
        start, end = index.products.temporal_extent("orthentick_produckt")

    start, end = index.products.temporal_extent(ls8_eo3_dataset.product)
    assert start == datetime.datetime(
        2013, 4, 4, 0, 58, 34, 682275,
        tzinfo=datetime.timezone.utc)
    assert end == datetime.datetime(
        2016, 5, 28, 23, 50, 59, 149573,
        tzinfo=datetime.timezone.utc)
    start2, end2 = index.products.temporal_extent(ls8_eo3_dataset.product.name)
    assert start == start2 and end == end2
    try:
        start2, end2 = index.datasets.temporal_extent([
            ls8_eo3_dataset.id, ls8_eo3_dataset2.id,
            ls8_eo3_dataset3.id, ls8_eo3_dataset4.id,
        ])
        assert start == start2 and end == end2
    except NotImplementedError:
        # Not implemented by postgres driver
        pass
