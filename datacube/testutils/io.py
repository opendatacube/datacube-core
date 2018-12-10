import numpy as np

from ..storage.storage import RasterFileDataSource, read_from_source
from ..utils.geometry import GeoBox


def rdr_geobox(rdr):
    """ Construct GeoBox from opened dataset reader.
    """
    h, w = rdr.shape
    return GeoBox(w, h, rdr.transform, rdr.crs)


def dc_read(path,
            band=1,
            gbox=None,
            resampling='nearest',
            dtype=None,
            dst_nodata=None,
            fallback_nodata=None):
    """
    Use default io driver to read file without constructing Dataset object.
    """
    source = RasterFileDataSource(path, band, nodata=fallback_nodata)
    with source.open() as rdr:
        dtype = rdr.dtype if dtype is None else dtype
        if gbox is None:
            gbox = rdr_geobox(rdr)
        if dst_nodata is None:
            dst_nodata = rdr.nodata

    # currently dst_nodata = None case is not supported. So if fallback_nodata
    # was None and file had none set, then use 0 as default output fill value
    if dst_nodata is None:
        dst_nodata = 0

    im = np.full(gbox.shape, dst_nodata, dtype=dtype)
    read_from_source(source, im, gbox.affine, dst_nodata, gbox.crs, resampling=resampling)
    return im
