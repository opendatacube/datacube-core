# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import csv
import datetime
import io
import uuid

import pytest
from click.testing import CliRunner
from dateutil import tz
from pathlib import Path

import datacube.scripts.search_tool
from datacube.model import Range

_EXAMPLE_LS7_NBAR_DATASET_FILE = Path(__file__).parent.joinpath('ls7-nbar-example.yaml')


@pytest.fixture
def pseudo_telemetry_type(index, default_metadata_type):
    index.datasets.types.add_document({
        'name': 'ls8_telemetry',
        'description': 'telemetry test',
        'metadata': {
            'product_type': 'pseudo_telemetry_data',
            'platform': {
                'code': 'LANDSAT_8'
            },
            'instrument': {
                'name': 'OLI_TIRS'
            },
            'format': {
                'name': 'PSEUDOMD'
            }
        },
        'metadata_type': default_metadata_type.name  # 'eo'
    })
    return index.datasets.types.get_by_name('ls8_telemetry')


@pytest.fixture
def pseudo_telemetry_dataset(index, db, pseudo_telemetry_type):
    id_ = str(uuid.uuid4())
    was_inserted = db.insert_dataset(
        {
            'id': id_,
            'product_type': 'pseudo_telemetry_data',
            'checksum_path': 'package.sha1',
            'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                        '116_074_20150330T022553Z20150330T022657',

            'ga_level': 'P00',
            'size_bytes': 637660782,
            'platform': {
                'code': 'LANDSAT_8'
            },
            # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
            'extent': {
                'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
                'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
                'coord': {
                    'll': {'lat': -31.33333, 'lon': 149.78434},
                    'lr': {'lat': -31.37116, 'lon': 152.20094},
                    'ul': {'lat': -29.23394, 'lon': 149.85216},
                    'ur': {'lat': -29.26873, 'lon': 152.21782}
                }
            },
            'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
            'instrument': {'name': 'OLI_TIRS'},
            'format': {
                'name': 'PSEUDOMD'
            },
            'lineage': {
                'source_datasets': {}
            }
        },
        id_,
        pseudo_telemetry_type.id
    )
    assert was_inserted
    d = index.datasets.get(id_)

    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_telemetry_type.id

    return d


def test_search_dataset_equals(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Wrong sensor name
    datasets = index.datasets.search_eager(
        platform='LANDSAT-8',
        instrument='TM',
    )
    assert len(datasets) == 0


def test_search_dataset_by_metadata(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_8"}, "instrument": {"name": "OLI_TIRS"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_5"}, "instrument": {"name": "TM"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 0


def test_search_dataset_ranges(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """

    # In the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Out of the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(28, 32),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 48, 0),
            datetime.datetime(2014, 7, 26, 23, 50, 0)
        )
    )
    assert len(datasets) == 0

    # Out of the time bounds
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 21, 48, 0),
            datetime.datetime(2014, 7, 26, 21, 50, 0)
        )
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # TODO: Do we want overlap as the default behaviour?
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.datasets.search_eager(
        lat=Range(-40, -30)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id


def test_search_globally(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # Insert dataset. It should be matched to the telemetry collection.
    # No expressions means get all.
    results = list(index.datasets.search())
    assert len(results) == 1


def test_search_by_product(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    """
    # Expect one product with our one dataset.
    products = list(index.datasets.search_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(products) == 1
    product, datasets = products[0]
    assert product.id == pseudo_telemetry_type.id
    assert next(datasets).id == pseudo_telemetry_dataset.id


def test_searches_only_type(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # One result in the metadata type
    datasets = index.datasets.search_eager(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # No results when searching for a different dataset type.
    datasets = index.datasets.search_eager(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 0

    # One result when no types specified.
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # No results for different metadata type.
    datasets = index.datasets.search_eager(
        metadata_type='telemetry',
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 0


def test_search_special_fields(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """

    # 'product' is a special case
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Unknown field: no results
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        flavour='chocolate',
    )
    assert len(datasets) == 0


def test_search_conflicting_types(index, pseudo_telemetry_dataset, pseudo_telemetry_type):
    # Should return no results.
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name,
        # The telemetry type is not of type storage_unit.
        metadata_type='storage_unit'
    )
    assert len(datasets) == 0


def test_fetch_all_of_md_type(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # Get every dataset of the md type.
    results = index.datasets.search_eager(
        metadata_type='eo'
    )
    assert len(results) == 1
    assert results[0].id == pseudo_telemetry_dataset.id
    # Get every dataset of the type.
    results = index.datasets.search_eager(
        product=pseudo_telemetry_dataset.type.name
    )
    assert len(results) == 1
    assert results[0].id == pseudo_telemetry_dataset.id

    # No results for another.
    results = index.datasets.search_eager(
        metadata_type='telemetry'
    )
    assert len(results) == 0


def test_count_searches(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.count(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # One result in the metadata type
    datasets = index.datasets.count(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results when searching for a different dataset type.
    datasets = index.datasets.count(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0

    # One result when no types specified.
    datasets = index.datasets.count(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results for different metadata type.
    datasets = index.datasets.count(
        metadata_type='telemetry',
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0


def test_count_by_product_searches(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    products = tuple(index.datasets.count_by_product(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # One result in the metadata type
    products = tuple(index.datasets.count_by_product(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # No results when searching for a different dataset type.
    products = tuple(index.datasets.count_by_product(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    ))
    assert products == ()

    # One result when no types specified.
    products = tuple(index.datasets.count_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # Only types with datasets should be returned (these params match ls5_gtiff too)
    products = tuple(index.datasets.count_by_product())
    assert products == ((pseudo_telemetry_type, 1),)

    # No results for different metadata type.
    products = tuple(index.datasets.count_by_product(
        metadata_type='telemetry',
    ))
    assert products == ()


def test_count_time_groups(index, pseudo_telemetry_type, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    """

    # 'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
    # 'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
    timeline = list(index.datasets.count_product_through_time(
        '1 day',
        product=pseudo_telemetry_type.name,
        time=Range(
            datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
            datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())
        )
    ))

    assert len(timeline) == 2
    assert timeline == [
        (
            Range(datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc())),
            0
        ),
        (
            Range(datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())),
            1
        )
    ]


def test_count_time_groups_cli(global_integration_cli_args, pseudo_telemetry_type, pseudo_telemetry_dataset):
    """
    Search datasets using the cli.
    :type global_integration_cli_args: tuple[str]
    :type default_metadata_type: datacube.model.Collection
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            'product-counts',
            '1 day',
            '2014-07-25 < time < 2014-07-27'
        ]
    )

    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts,
        catch_exceptions=False
    )
    assert result.exit_code == 0

    expected_out = (
        '{}\n'
        '    2014-07-25: 0\n'
        '    2014-07-26: 1\n'
    ).format(pseudo_telemetry_type.name)

    assert result.output == expected_out


def test_search_cli_basic(global_integration_cli_args, default_metadata_type, pseudo_telemetry_dataset):
    """
    Search datasets using the cli.
    :type global_integration_cli_args: tuple[str]
    :type default_metadata_type: datacube.model.Collection
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            # No search arguments: return all datasets.
            'datasets'
        ]
    )

    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts
    )
    assert str(pseudo_telemetry_dataset.id) in result.output
    assert str(default_metadata_type.name) in result.output

    assert result.exit_code == 0


def _cli_csv_search(args, global_integration_cli_args):
    global_opts = list(global_integration_cli_args)
    global_opts.extend(['-f', 'csv'])
    result = _cli_search(args, global_opts)
    assert result.exit_code == 0
    return list(csv.DictReader(io.StringIO(result.output)))


def _cli_search(args, global_integration_cli_args):
    opts = list(global_integration_cli_args)
    opts.extend(args)
    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts,
        catch_exceptions=False
    )
    return result
