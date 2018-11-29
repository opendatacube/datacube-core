from datacube import Datacube
from datacube.api.query import query_group_by
import numpy as np

from pathlib import Path
from datacube.testutils import (
    mk_sample_dataset,
    mk_test_image,
    write_gtiff,
)


def test_load_data(tmpdir):
    tmpdir = Path(str(tmpdir))

    nodata = -999
    aa = mk_test_image(96, 64, 'int16', -999)
    fname = 'aa.tiff'

    meta = write_gtiff(tmpdir/fname, aa,
                       nodata=nodata,
                       resolution=(15, -15),
                       offset=(11230, 1381110),
                       overwrite=True)

    gbox = meta['gbox']

    assert gbox.transform[0] == 15
    assert (tmpdir/fname).exists()

    bands = [dict(name=n,
                  path=n+'.tiff',
                  layer=1,
                  dtype='int16',
                  nodata=nodata)
             for n in 'aa bb cc'.split(' ')]

    uri = Path(tmpdir/'metadata.yaml').absolute().as_uri()
    ds = mk_sample_dataset(bands, uri=uri, timestamp='2018-07-19')
    assert ds.time is not None

    group_by = query_group_by('time')
    sources = Datacube.group_datasets([ds], group_by)

    mm = ['aa']
    mm = [ds.type.measurements[k] for k in mm]

    ds_data = Datacube.load_data(sources, gbox, mm)
    assert ds_data.aa.nodata == nodata
    np.testing.assert_array_equal(aa, ds_data.aa.values[0])
