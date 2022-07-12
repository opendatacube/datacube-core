import pytest

from unittest.mock import MagicMock

from datacube.index.hl import Doc2Dataset


def test_support_validation(non_geo_dataset_doc, eo_dataset_doc):
    idx = MagicMock()

    idx.supports_legacy = False
    idx.supports_nongeo = False
    with pytest.raises(ValueError, match="EO3 cannot be set to False"):
        resolver = Doc2Dataset(idx, eo3=False)

    idx.supports_legacy = True
    idx.supports_nongeo = False
    resolver = Doc2Dataset(idx, products=["product_a"], eo3=False)
    _, err = resolver(non_geo_dataset_doc, "//location/")
    assert "Non-geospatial metadata formats" in err

    idx.supports_legacy = False
    idx.supports_nongeo = True
    resolver = Doc2Dataset(idx, products=["product_a"], eo3=False)
    _, err = resolver(eo_dataset_doc, "//location/")
    assert "Legacy metadata formats" in err
