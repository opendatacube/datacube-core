# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest
from uuid import UUID, uuid4 as random_uuid

from datacube.model import LineageDirection, LineageTree, InconsistentLineageException
from datacube.model.lineage import InconsistentLineageException, LineageRelations, LineageRelation

def test_ltree_clsmethods():
    root = random_uuid()
    minimal = LineageTree.from_eo3_doc(dsid=root)
    assert minimal.dataset_id == root
    assert minimal.direction == LineageDirection.SOURCES
    assert minimal.children is None
    optional_args = LineageTree.from_eo3_doc(dsid=root, sources={},
                                          direction=LineageDirection.DERIVED,
                                          home="notused")
    assert optional_args == LineageTree(direction=LineageDirection.DERIVED,
                                        dataset_id=root,
                                        children={},
                                        home=None
                                        )

@pytest.fixture
def big_src_tree_ids():
    return {
        "root": random_uuid(),
        "ard1": random_uuid(),
        "ard2": random_uuid(),

        "l1_1": random_uuid(),
        "l1_2": random_uuid(),
        "l1_3": random_uuid(),

        "l1_4": random_uuid(),
        "l1_5": random_uuid(),
        "l1_6": random_uuid(),

        "atmos": random_uuid(),
    }


@pytest.fixture
def big_src_lineage_tree(big_src_tree_ids):
    ids = big_src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(dataset_id=ids["l1_1"], direction=direction,
                                children={}
                            ),
                            LineageTree(dataset_id=ids["l1_2"], direction=direction,
                                children={}
                            ),
                            LineageTree(dataset_id=ids["l1_3"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(dataset_id=ids["atmos"], direction=direction,
                                children={}
                            )
                        ],
                    }
                ),
                LineageTree(dataset_id=ids["ard2"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(dataset_id=ids["l1_4"], direction=direction,
                                children={}
                            ),
                            LineageTree(dataset_id=ids["l1_5"], direction=direction,
                                children={}
                            ),
                            LineageTree(dataset_id=ids["l1_6"], direction=direction,
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(dataset_id=ids["atmos"], direction=direction,
                                children={}
                            )
                        ],
                    }
                ),
            ]
        }
    )


def test_lin_rels_minimal(big_src_lineage_tree):
    rels1 = LineageRelations(tree=big_src_lineage_tree)
    repeated_uuid = random_uuid()
    looped_tree = LineageTree(
        dataset_id=repeated_uuid, direction=LineageDirection.SOURCES, children={
            "cyclic_self": [
                LineageTree(
                    dataset_id=repeated_uuid, direction=LineageDirection.SOURCES, children={}
                )
            ]
        }
    )
    with pytest.raises(InconsistentLineageException) as e:
        rels2 = LineageRelations(looped_tree)
    assert "foobar" in str(e.value)
