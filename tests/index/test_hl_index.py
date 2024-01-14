# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from unittest.mock import MagicMock

from datacube.index.hl import Doc2Dataset


def test_support_validation(non_geo_dataset_doc, eo_dataset_doc):
    idx = MagicMock()

    idx.supports_legacy = False
    idx.supports_nongeo = False
    with pytest.raises(ValueError, match="EO3 cannot be set to False"):
        resolver = Doc2Dataset(idx, eo3=False)

    with pytest.raises(ValueError, match="fail_on_missing_lineage is not supported for this index driver"):
        resolver = Doc2Dataset(idx, fail_on_missing_lineage=True)

    idx.supports_lineage = True
    idx.supports_external_lineage = True
    with pytest.raises(ValueError, match="Cannot provide a default home_index when skip_lineage"):
        resolver = Doc2Dataset(idx, home_index="right_here", skip_lineage=True)

    idx.supports_legacy = True
    idx.supports_nongeo = False
    idx.supports_external_lineage = False
    resolver = Doc2Dataset(idx, products=["product_a"], eo3=False)
    _, err = resolver(non_geo_dataset_doc, "//location/")
    assert "Non-geospatial metadata formats" in err

    idx.supports_legacy = False
    idx.supports_nongeo = True
    idx.supports_external_lineage = False
    resolver = Doc2Dataset(idx, products=["product_a"], eo3=False)
    _, err = resolver(eo_dataset_doc, "//location/")
    assert "Legacy metadata formats" in err
