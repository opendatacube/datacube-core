# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from uuid import uuid4 as random_uuid

import pytest

from datacube.model import LineageDirection, LineageTree, InconsistentLineageException
from datacube.model.lineage import LineageRelations


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_lineage_home_api(index):
    a_uuids = [random_uuid() for i in range(10)]
    b_uuids = [random_uuid() for i in range(10)]
    all_uuids = a_uuids + b_uuids
    assert index.lineage.get_homes(*a_uuids) == {}
    # Test delete of non-existent entries
    assert index.lineage.clear_home(*a_uuids) == 0
    # Test insert a uuids
    assert index.lineage.set_home("spam", *a_uuids) == 10
    for home in index.lineage.get_homes(*a_uuids).values():
        assert home == "spam"
    # Test update with and without allow_update
    index.lineage.set_home("eggs", *a_uuids) == 0
    index.lineage.set_home("eggs", *a_uuids, allow_updates=True) == 10
    for home in index.lineage.get_homes(*a_uuids).values():
        assert home == "eggs"
    assert index.lineage.get_homes(*b_uuids) == {}
    index.lineage.set_home("eggs", *a_uuids, allow_updates=True) == 0
    index.lineage.set_home("eggs", *b_uuids, allow_updates=True) == 10

    # Test clear_home with actual work done.
    assert index.lineage.clear_home(*a_uuids) == 10
    assert index.lineage.clear_home(*b_uuids) == 10


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_lineage_merge(index, src_lineage_tree, compatible_derived_tree):
    stree, ids = src_lineage_tree
    dtree, ids = compatible_derived_tree

    rels = LineageRelations(tree=stree)
    rels.merge_tree(dtree)
    index.lineage.merge(rels)
    src_tree = index.lineage.get_source_tree(ids["root"])
    assert src_tree.dataset_id == ids["root"]
    assert src_tree.direction == LineageDirection.SOURCES
    for ard_subtree in src_tree.children["ard"]:
        assert ard_subtree.dataset_id in (ids["ard1"], ids["ard2"])


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_lineage_tree_index_api_simple(index, src_lineage_tree):
    tree, ids = src_lineage_tree
    # Test api responses for lineage not in database:
    src_tree = index.lineage.get_source_tree(ids["root"])
    assert src_tree.dataset_id == ids["root"]
    assert src_tree.direction == LineageDirection.SOURCES
    assert src_tree.children == {}
    # Add the test tree to depth 1
    index.lineage.add(tree, max_depth=1)
    src_tree = index.lineage.get_source_tree(ids["root"])
    assert src_tree.dataset_id == ids["root"]
    assert src_tree.direction == LineageDirection.SOURCES
    for ard_subtree in src_tree.children["ard"]:
        assert ard_subtree.dataset_id in (ids["ard1"], ids["ard2"])
        assert not ard_subtree.children
    # Add the test tree to depth 2
    index.lineage.add(tree, max_depth=2)
    src_tree = index.lineage.get_source_tree(ids["root"])
    for ard_subtree in src_tree.children["ard"]:
        assert "l1" in ard_subtree.children
        assert not ard_subtree.children["atmos_corr"][0].children
    # Add full test tree
    index.lineage.add(tree, max_depth=0)
    src_tree = index.lineage.get_source_tree(ids["root"])
    seen = False
    for ard_subtree in src_tree.children["ard"]:
        assert "l1" in ard_subtree.children
        assert "atmos_corr" in ard_subtree.children
        if ard_subtree.children["atmos_corr"][0].children:
            assert "preatmos" in ard_subtree.children["atmos_corr"][0].children
            seen = True
    assert seen
    # And test reversing the tree
    der_tree = index.lineage.get_derived_tree(ids["atmos_parent"])
    assert der_tree.find_subtree(ids["root"]).dataset_id == ids["root"]
    # Test Lineage removal - sourcewards
    index.lineage.remove(ids["root"], LineageDirection.SOURCES, max_depth=2)
    src_tree = index.lineage.get_source_tree(ids["root"])
    assert not src_tree.children
    src_tree = index.lineage.get_source_tree(ids["atmos"])
    assert src_tree.children
    # Test Lineage removal - derivedwards
    index.lineage.add(tree, max_depth=0)
    index.lineage.remove(ids["atmos_parent"], LineageDirection.DERIVED, max_depth=2)
    src_tree = index.lineage.get_source_tree(ids["root"])
    assert src_tree.children
    src_tree = index.lineage.get_source_tree(ids["atmos"])
    assert not src_tree.children


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_lineage_tree_index_api_consistent(index, src_lineage_tree, compatible_derived_tree):
    tree1, ids = src_lineage_tree
    tree2, ids = compatible_derived_tree

    index.lineage.add(tree1)
    tree1a = index.lineage.get_source_tree(ids["root"])
    assert tree1a.home is None

    index.lineage.add(tree2)
    tree2a = index.lineage.get_source_tree(ids["root"])
    assert tree2a.home == "extensions"
    tree2b = tree2a.find_subtree(ids["atmos"])
    tree2c = tree2b.find_subtree(ids["atmos_grandparent"])
    assert tree2c


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_lineage_tree_index_api_inconsistent_homes(index, src_lineage_tree):
    tree, ids = src_lineage_tree
    home_update = LineageTree(
        dataset_id=ids["ard1"],
        direction=LineageDirection.SOURCES,
        home="not_too_ard",
        children={
            "l1": [
                LineageTree(
                    dataset_id=ids["l1_1"],
                    direction=LineageDirection.SOURCES,
                    home="level1a",
                )
            ]
        }
    )
    index.lineage.add(tree)
    with pytest.raises(InconsistentLineageException):
        index.lineage.add(home_update, allow_updates=False)
    index.lineage.add(home_update, allow_updates=True)
    dbtree = index.lineage.get_source_tree(ids["ard1"])
    assert dbtree.home == "not_too_ard"


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_get_extensions(index, dataset_with_external_lineage):
    dataset, src_lineage_tree, derived_lineage_tree, ids = dataset_with_external_lineage

    ds = index.datasets.get(ids["root"])
    assert ds.source_tree is None
    assert ds.derived_tree is None

    ds = index.datasets.get(ids["root"], include_sources=True)
    assert ds.source_tree is not None
    assert ds.derived_tree is None
    assert ds.source_tree.children["ard"][0].children

    ds = index.datasets.get(ids["root"], include_sources=True, max_depth=1)
    assert ds.source_tree is not None
    assert ds.derived_tree is None
    assert not ds.source_tree.children["ard"][0].children

    ds = index.datasets.get(ids["root"], include_deriveds=True)
    assert ds.source_tree is None
    assert ds.derived_tree is not None
    assert ds.derived_tree.children["dra"][0].children

    ds = index.datasets.get(ids["root"], include_deriveds=True, max_depth=1)
    assert ds.source_tree is None
    assert ds.derived_tree is not None
    assert not ds.derived_tree.children["dra"][0].children

    ds = index.datasets.get(ids["root"], include_sources=True, include_deriveds=True)
    assert ds.source_tree is not None
    assert ds.derived_tree is not None
    assert ds.source_tree.children["ard"][0].children
    assert ds.derived_tree.children["dra"][0].children

    ds = index.datasets.get(ids["root"], include_sources=True, include_deriveds=True, max_depth=1)
    assert ds.source_tree is not None
    assert ds.derived_tree is not None
    assert not ds.source_tree.children["ard"][0].children
    assert not ds.derived_tree.children["dra"][0].children
