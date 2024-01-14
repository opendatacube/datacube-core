# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import os
import pytest
from uuid import uuid4 as random_uuid

from datacube.model import LineageDirection, LineageTree, InconsistentLineageException
from datacube.model.lineage import LineageRelations, LineageIDPair
from datacube.utils import read_documents


def test_directions():
    assert LineageDirection.SOURCES != LineageDirection.DERIVED
    assert LineageDirection.DERIVED == LineageDirection.SOURCES.opposite()
    assert LineageDirection.SOURCES == LineageDirection.DERIVED.opposite()


def test_ltree_clsmethods(data_folder):
    root = random_uuid()
    # Minimal tree - root node only
    minimal = LineageTree.from_data(dsid=root)
    assert minimal.dataset_id == root
    assert minimal.direction == LineageDirection.SOURCES
    assert minimal.children is None
    # Check optional args to from_eo3_doc are set correctly.
    optional_args = LineageTree.from_data(dsid=root, sources={},
                                          direction=LineageDirection.DERIVED,
                                          home="notused")
    assert optional_args == LineageTree(direction=LineageDirection.DERIVED,
                                        dataset_id=root,
                                        children={},
                                        home=None)   # Note home is not written to the root node
    doc = list(
        read_documents(
            str(os.path.join(data_folder, "ds_eo3.yml"))
        )
    )[0][1]
    tree = LineageTree.from_eo3_doc(doc, home="src_home", home_derived="der_home")
    assert tree.home == "der_home"
    for child in tree.children["bc"]:
        assert child.home == "src_home"


@pytest.fixture
def shared_tree_ids():
    return {
        "ard1": random_uuid(),

        "l1_1": random_uuid(),
        "l1_2": random_uuid(),
        "l1_3": random_uuid(),

        "atmos": random_uuid(),
    }


@pytest.fixture
def src_tree_ids(shared_tree_ids):
    return {
        "root": random_uuid(),
        "ard1": shared_tree_ids["ard1"],

        "l1_1": shared_tree_ids["l1_1"],
        "l1_2": shared_tree_ids["l1_2"],
        "l1_3": shared_tree_ids["l1_3"],

        "atmos": shared_tree_ids["atmos"],
    }


@pytest.fixture
def src_lineage_tree(src_tree_ids):
    ids = src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                home="level1db",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                home="level1db",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                home="level1db",
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                home="level1db",
                                children=None
                            )
                        ],
                    }
                ),
            ]
        }
    )


def test_lineage_serialisation(src_lineage_tree, src_tree_ids):
    ids = src_tree_ids
    serialised = src_lineage_tree.serialise()
    assert serialised == {
        "id": str(ids["root"]),
        "sources": {
            "ard": [
                {
                    "id": str(ids["ard1"]),
                    "sources": {
                        "l1": [
                            {
                                "id": str(ids["l1_1"]),
                                "home": "level1db"
                            },
                            {
                                "id": str(ids["l1_2"]),
                                "home": "level1db"
                            },
                            {
                                "id": str(ids["l1_3"]),
                                "home": "level1db"
                            },
                        ],
                        "atmos_corr": [
                            {
                                "id": str(ids["atmos"]),
                                "home": "level1db"
                            }
                        ]
                    },
                }
            ]
        }
    }
    tree_out = LineageTree.deserialise(serialised)
    assert tree_out == src_lineage_tree


@pytest.fixture
def src_lineage_tree_diffhome(src_tree_ids):
    ids = src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                home="elsewhere",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                home="elsewhere",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children=None
                            )
                        ],
                    }
                ),
            ]
        }
    )


@pytest.fixture
def mixed_dir_lineage_tree(src_tree_ids):
    ids = src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=LineageDirection.DERIVED,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children=None
                            )
                        ],
                    }
                ),
            ]
        }
    )


@pytest.fixture
def big_src_tree_ids(shared_tree_ids):
    ids = shared_tree_ids
    return {
        "root": random_uuid(),
        "ard1": ids["ard1"],
        "ard2": random_uuid(),

        "l1_1": ids["l1_1"],
        "l1_2": ids["l1_2"],
        "l1_3": ids["l1_3"],

        "l1_4": random_uuid(),
        "l1_5": random_uuid(),
        "l1_6": random_uuid(),

        "atmos": ids["atmos"],
        "atmos_parent": random_uuid()
    }


@pytest.fixture
def big_src_lineage_tree(big_src_tree_ids):
    ids = big_src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children=None
                            )
                        ],
                    }
                ),
                LineageTree(
                    dataset_id=ids["ard2"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_4"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_5"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_6"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children={
                                    "preatmos": [
                                        LineageTree(
                                            dataset_id=ids["atmos_parent"], direction=direction,
                                            children={}
                                        )
                                    ]
                                }
                            )
                        ],
                    }
                ),
            ]
        }
    )


@pytest.fixture
def classifier_mismatch(big_src_tree_ids):
    ids = big_src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children=None
                            )
                        ],
                    }
                ),
                LineageTree(
                    dataset_id=ids["ard2"], direction=direction,
                    children={
                        "lvl1": [
                            LineageTree(
                                dataset_id=ids["l1_4"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_5"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_6"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children={
                                    "preatmos": [
                                        LineageTree(
                                            dataset_id=ids["atmos_parent"], direction=direction,
                                            children={}
                                        )
                                    ]
                                }
                            )
                        ],
                    }
                ),
            ]
        }
    )


@pytest.fixture
def src_lineage_tree_with_bad_diamond(big_src_tree_ids):
    ids = big_src_tree_ids
    direction = LineageDirection.SOURCES
    # This is a test tree with a malformed diamond relationship.
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            # First half of the diamond relationship:
                            # ard1 depends on atmos (which depends on atmos_parent)
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children={
                                    "preatmos": [
                                        LineageTree(
                                            dataset_id=ids["atmos_parent"], direction=direction,
                                            children={}
                                        )
                                    ]
                                }
                            )
                        ],
                    }
                ),
                LineageTree(
                    dataset_id=ids["ard2"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_4"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_5"], direction=direction,
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_6"], direction=direction,
                                children={}
                            ),
                        ],
                        # Second half of the diamond relationship:
                        # ard2 also depends on atmos (which depends on atmos_parent)
                        # But the atmos->atmos_parent relationship was already recorded
                        # above, so "children" here should be None or {}.
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                children={
                                    "preatmos": [
                                        LineageTree(
                                            dataset_id=ids["atmos_parent"], direction=direction,
                                            children={}
                                        )
                                    ]
                                }
                            )
                        ],
                    }
                ),
            ]
        }
    )


def test_child_datasets(big_src_lineage_tree, big_src_tree_ids):
    cds = big_src_lineage_tree.child_datasets()
    for dsid in big_src_tree_ids.values():
        assert dsid == big_src_lineage_tree.dataset_id or dsid in cds


def test_lin_rels_lin_tree_conversions(big_src_lineage_tree, big_src_tree_ids):
    # Create LRS from LT
    rels1 = LineageRelations(tree=big_src_lineage_tree)
    # Extract LT from LRS
    extracted_tree = rels1.extract_tree(big_src_tree_ids["root"], big_src_lineage_tree.direction)
    # Confirm extract LT produces same LRS as original LT
    rels2 = LineageRelations(tree=extracted_tree)
    for rel in rels1.relations:
        assert rel in rels2.relations
    # Extract reverse direction tree
    derived_tree = rels1.extract_tree(root=big_src_tree_ids["atmos"], direction=LineageDirection.DERIVED)
    rels3 = LineageRelations(tree=derived_tree)
    extracted_tree = rels3.extract_tree(big_src_tree_ids["root"], direction=big_src_lineage_tree.direction)
    rels4 = LineageRelations(tree=extracted_tree)
    for rel in rels4.relations:
        assert rel in rels1.relations


def test_detect_cyclic_deps(big_src_lineage_tree, big_src_tree_ids):
    # Confirm trivial cyclic dependencies are detected.
    repeated_uuid = random_uuid()
    looped_tree = LineageTree(
        dataset_id=repeated_uuid, direction=LineageDirection.SOURCES,
        children={
            "cyclic_self": [
                LineageTree(dataset_id=repeated_uuid, direction=LineageDirection.SOURCES, children={})
            ]
        }
    )
    with pytest.raises(InconsistentLineageException) as e:
        rels2 = LineageRelations(tree=looped_tree)
    assert "LineageTrees must be acyclic" in str(e.value)
    # Confirm more complex cyclic dependencies can be detected.
    rels = LineageRelations(tree=big_src_lineage_tree)
    breaking_tree = LineageTree(
        dataset_id=big_src_tree_ids["l1_1"],
        direction=LineageDirection.SOURCES,
        children={
            "cyclic_dep": [LineageTree(
                dataset_id=big_src_tree_ids["root"],
                direction=LineageDirection.SOURCES
            )]
        }
    )
    with pytest.raises(InconsistentLineageException) as e:
        rels.merge_tree(breaking_tree)
    assert "LineageTrees must be acyclic" in str(e.value)


def test_subtree(big_src_lineage_tree, big_src_tree_ids, src_lineage_tree, src_tree_ids):
    sub = big_src_lineage_tree.find_subtree(big_src_tree_ids["root"])
    assert sub == big_src_lineage_tree

    # Test multiple nodes, one with children
    sub = big_src_lineage_tree.find_subtree(big_src_tree_ids["atmos"])
    assert sub.dataset_id == big_src_tree_ids["atmos"]
    assert sub.children is not None

    # Test no nodes with children
    sub = src_lineage_tree.find_subtree((src_tree_ids["atmos"]))
    assert sub.dataset_id == src_tree_ids["atmos"]
    assert sub.children is None


def test_good_consistency_check(big_src_lineage_tree, src_lineage_tree, big_src_tree_ids):
    rels1 = LineageRelations(tree=src_lineage_tree)
    rels2 = LineageRelations(tree=big_src_lineage_tree)
    diff = rels1.relations_diff(rels2)
    assert diff[1] == {} and diff[3] == {}
    assert LineageIDPair(derived_id=src_lineage_tree.dataset_id, source_id=big_src_tree_ids["ard1"]) in diff[0]
    diff = rels1.relations_diff(rels2, allow_updates=True)
    assert diff[1] == {} and diff[3] == {}
    assert LineageIDPair(derived_id=src_lineage_tree.dataset_id, source_id=big_src_tree_ids["ard1"]) in diff[0]
    diff = rels1.relations_diff()
    assert diff[1] == {} and diff[3] == {}
    assert LineageIDPair(derived_id=src_lineage_tree.dataset_id, source_id=big_src_tree_ids["ard1"]) in diff[0]


def test_bad_diamond(src_lineage_tree_with_bad_diamond, big_src_tree_ids):
    # Test detection of trees that have a diamond relationship in which both paths are extended.
    # E.g. If A->B->C->D  and A->E->C->D, the C->D relationship should only be recorded
    # under one branch (B or A) and the other occurence of C should have no children recorded.
    with pytest.raises(InconsistentLineageException, match="Duplicate nodes in LineageTree"):
        rels = LineageRelations(tree=src_lineage_tree_with_bad_diamond)


def test_home_mismatch(big_src_lineage_tree):
    tree = big_src_lineage_tree
    tree.children["ard"][0].children["atmos_corr"][0].home = "bungalow"
    tree.children["ard"][1].children["atmos_corr"][0].home = "apartment"
    with pytest.raises(InconsistentLineageException, match="Tree contains inconsistent homes"):
        rels = LineageRelations(tree=tree)


def test_classifier_mismatch(big_src_lineage_tree, classifier_mismatch):
    rels1 = LineageRelations(tree=big_src_lineage_tree)
    rels2 = LineageRelations(tree=classifier_mismatch)
    with pytest.raises(
            InconsistentLineageException,
            match="Dataset .* is derived from .* with inconsistent classifiers."):
        rels1.merge(rels2)


def test_classifier_update(big_src_lineage_tree, classifier_mismatch):
    rels1 = LineageRelations(tree=big_src_lineage_tree)
    rels2 = LineageRelations(tree=classifier_mismatch)
    diffs = rels1.relations_diff(existing_relations=rels2, allow_updates=True)
    assert len(diffs[1]) > 0


def test_home_update(src_lineage_tree, src_lineage_tree_diffhome):
    rels1 = LineageRelations(tree=src_lineage_tree)
    rels2 = LineageRelations(tree=src_lineage_tree_diffhome)
    diffs = rels1.relations_diff(existing_relations=rels2, allow_updates=True)
    assert len(diffs[3]) > 0


def test_mixed_dirs(mixed_dir_lineage_tree):
    with pytest.raises(InconsistentLineageException, match="Tree contains both derived and source nodes"):
        rels1 = LineageRelations(tree=mixed_dir_lineage_tree)


def test_merge_tree_limited_depth(big_src_lineage_tree, big_src_tree_ids):
    ids = big_src_tree_ids
    rels = LineageRelations(tree=big_src_lineage_tree, max_depth=1)
    assert ids["root"] in rels.dataset_ids
    assert ids["ard1"] in rels.dataset_ids
    assert ids["l1_1"] not in rels.dataset_ids
    assert ids["atmos"] not in rels.dataset_ids
    assert ids["atmos_parent"] not in rels.dataset_ids

    rels = LineageRelations(tree=big_src_lineage_tree, max_depth=2)
    assert ids["root"] in rels.dataset_ids
    assert ids["ard1"] in rels.dataset_ids
    assert ids["l1_1"] in rels.dataset_ids
    assert ids["atmos"] in rels.dataset_ids
    assert ids["atmos_parent"] not in rels.dataset_ids

    rels = LineageRelations(tree=big_src_lineage_tree, max_depth=3)
    assert ids["root"] in rels.dataset_ids
    assert ids["ard1"] in rels.dataset_ids
    assert ids["l1_1"] in rels.dataset_ids
    assert ids["atmos"] in rels.dataset_ids
    assert ids["atmos_parent"] in rels.dataset_ids

    rels = LineageRelations(tree=big_src_lineage_tree, max_depth=7)
    assert ids["atmos_parent"] in rels.dataset_ids
