from unittest.mock import MagicMock

import pytest

from datacube.index.hl import Doc2Dataset


def test_eo3_doc2ds_check():
    idx = MagicMock()
    idx.supports_legacy = False
    idx.supports_nongeo = False

    with pytest.raises(ValueError, match="EO3 cannot be set to False"):
        resolver = Doc2Dataset(idx, eo3=False)
