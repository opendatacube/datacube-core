from unittest.mock import MagicMock

import pytest

from datacube.index.abstract import AbstractIndex
from datacube.index.hl import Doc2Dataset


def test_eo3_doc2ds_check():
    idx = MagicMock()
    idx.supports_legacy = False

    with pytest.raises(ValueError):
        resolver = Doc2Dataset(idx, eo3=False)
