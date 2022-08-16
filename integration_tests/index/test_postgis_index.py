import pytest

from datacube.index import Index
from datacube.model import Dataset
from datacube.model import Product
from datacube.utils.geometry import CRS

@pytest.mark.parametrize('datacube_env_name', ('experimental', ))
def test_spatial_index(index: Index):
    assert list(index.spatial_indexes()) == []
    # WKT CRS which cannot be mapped to an EPSG number.
    assert not index.create_spatial_index(CRS('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Weird",22.3],UNIT["Degree",0.017453292519943295]]'))
    assert index.create_spatial_index(CRS("EPSG:4326"))
