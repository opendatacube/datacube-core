# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest

from datacube.model import Dataset
from datacube.testutils import suppress_deprecations


@pytest.mark.parametrize("datacube_env_name", ("datacube",))
def test_legacy_location_behaviour(index, ls8_eo3_dataset) -> None:
    with suppress_deprecations():
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [ls8_eo3_dataset.uri]
        update = Dataset(  # Test of deprecated behaviour
            ls8_eo3_dataset.product,
            ls8_eo3_dataset.metadata_doc,
            uris=locations + ["file:/tmp/foo"],
        )
        index.datasets.update(update)
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert index.datasets.get_location(ls8_eo3_dataset.id) == locations[0]
        assert locations == ["file:/tmp/foo", ls8_eo3_dataset.uri]
        index.datasets.add_location(ls8_eo3_dataset.id, "s3:/bucket/hole/straw.axe")
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [
            "s3:/bucket/hole/straw.axe",
            "file:/tmp/foo",
            ls8_eo3_dataset.uri,
        ]
        index.datasets.archive_location(ls8_eo3_dataset.id, "file:/tmp/foo")
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [
            "s3:/bucket/hole/straw.axe",
            ls8_eo3_dataset.uri,
        ]
        assert "file:/tmp/foo" in index.datasets.get_archived_locations(
            ls8_eo3_dataset.id
        )
        assert (
            "file:/tmp/foo"
            == index.datasets.get_archived_location_times(ls8_eo3_dataset.id)[0][0]
        )
        index.datasets.restore_location(ls8_eo3_dataset.id, "file:/tmp/foo")
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [
            "s3:/bucket/hole/straw.axe",
            "file:/tmp/foo",
            ls8_eo3_dataset.uri,
        ]
        index.datasets.remove_location(ls8_eo3_dataset.id, "file:/tmp/foo")
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [
            "s3:/bucket/hole/straw.axe",
            ls8_eo3_dataset.uri,
        ]
        index.datasets.remove_location(ls8_eo3_dataset.id, "s3:/bucket/hole/straw.axe")
        index.datasets.remove_location(ls8_eo3_dataset.id, ls8_eo3_dataset.uri)
        ls8_eo3_dataset = index.datasets.get(ls8_eo3_dataset.id)
        assert ls8_eo3_dataset.uri is None
        assert index.datasets.get_location(ls8_eo3_dataset.id) is None


@pytest.mark.parametrize("datacube_env_name", ("postgis",))
def test_postgis_no_multiple_locations(index, ls8_eo3_dataset) -> None:
    with suppress_deprecations():
        locations = index.datasets.get_locations(
            ls8_eo3_dataset.id
        )  # Test of deprecated method
        assert locations == [ls8_eo3_dataset.uri]

        update = Dataset(
            ls8_eo3_dataset.product,
            ls8_eo3_dataset.metadata_doc,
            uris=locations + ["file:/tmp/foo"],
        )
        with pytest.raises(ValueError):
            index.datasets.update(update)
        assert index.datasets.get_location(ls8_eo3_dataset.id) == ls8_eo3_dataset.uri

        index.datasets.remove_location(ls8_eo3_dataset.id, "file:/tmp/foo")
        assert index.datasets.get_location(ls8_eo3_dataset.id) == ls8_eo3_dataset.uri

        index.datasets.remove_location(ls8_eo3_dataset.id, ls8_eo3_dataset.uri)
    ls8_eo3_dataset = index.datasets.get(ls8_eo3_dataset.id)
    assert ls8_eo3_dataset.uri is None
    assert index.datasets.get_location(ls8_eo3_dataset.id) is None

    with suppress_deprecations():
        index.datasets.add_location(ls8_eo3_dataset.id, "file:/tmp/foo")
        location = index.datasets.get_location(ls8_eo3_dataset.id)
        assert location == "file:/tmp/foo"

        with pytest.raises(ValueError):
            index.datasets.add_location(ls8_eo3_dataset.id, "s3:/bucket/hole/straw.axe")

        assert index.datasets.get_archived_locations(ls8_eo3_dataset.id) == []


def test_dataset_tuple_uris(ls8_eo3_product) -> None:
    from datacube.index.abstract import DatasetTuple

    dst1 = DatasetTuple(ls8_eo3_product, {"dummy": True}, "file:///uri1")
    dst2 = DatasetTuple(
        ls8_eo3_product, {"dummy": True}, ["file:///uri1", "https://host.domain/uri1"]
    )

    with suppress_deprecations():
        assert dst1.uri == dst2.uri
        assert dst1.uri == dst2.uri
        assert dst1.uris == [dst1.uri]
        assert dst2.uri in dst2.uris
    assert not dst1.is_legacy
    assert dst2.is_legacy
