import numpy as np

from ..storage import reproject_and_fuse, BandInfo
from ..storage._rio import RasterioDataSource, RasterDatasetDataSource
from ..utils.geometry._warp import resampling_s2rio
from ..storage._read import rdr_geobox
from ..utils.geometry import GeoBox
from ..utils.geometry import gbox as gbx
from types import SimpleNamespace


class RasterFileDataSource(RasterioDataSource):
    """ This is only used in test code
    """
    def __init__(self, filename, bandnumber, nodata=None, crs=None, transform=None, lock=None):
        super(RasterFileDataSource, self).__init__(filename, nodata, lock=lock)
        self.bandnumber = bandnumber
        self.crs = crs
        self.transform = transform

    def get_bandnumber(self, src):
        return self.bandnumber

    def get_transform(self, shape):
        if self.transform is None:
            raise RuntimeError('No transform in the data and no fallback')
        return self.transform

    def get_crs(self):
        if self.crs is None:
            raise RuntimeError('No CRS in the data and no fallback')
        return self.crs


def _raster_metadata(band):
    source = RasterDatasetDataSource(band)
    with source.open() as rdr:
        return SimpleNamespace(dtype=rdr.dtype.name,
                               nodata=rdr.nodata,
                               geobox=rdr_geobox(rdr))


def get_raster_info(ds, measurements=None):
    """
    :param ds: Dataset
    :param measurements: List of band names to load
    """
    if measurements is None:
        measurements = list(ds.type.measurements)

    return {n: _raster_metadata(BandInfo(ds, n))
            for n in measurements}


def native_geobox(ds, measurements=None, basis=None):
    """Compute native GeoBox for a set of bands for a given dataset

    :param ds: Dataset
    :param measurements: List of band names to consider
    :param basis: Name of the band to use for computing reference frame, other
    bands might be reprojected if they use different pixel grid

    :return: GeoBox describing native storage coordinates.
    """
    gs = ds.type.grid_spec
    if gs is not None:
        # Dataset is from ingested product, figure out GeoBox of the tile this dataset covers
        bb = [gbox for _, gbox in gs.tiles(ds.bounds)]
        if len(bb) != 1:
            # Ingested product but dataset overlaps several/none tiles -- no good
            raise ValueError('Broken GridSpec detected')
        return bb[0]

    if basis is not None:
        return get_raster_info(ds, [basis])[basis].geobox

    ii = get_raster_info(ds, measurements)
    gboxes = [info.geobox for info in ii.values()]
    geobox = gboxes[0]
    consistent = all(geobox == gbox for gbox in gboxes)
    if not consistent:
        raise ValueError('Not all bands share the same pixel grid')
    return geobox


def native_load(ds, measurements=None, basis=None, **kw):
    """Load single dataset in native resolution.

    :param ds: Dataset
    :param measurements: List of band names to load
    :param basis: Name of the band to use for computing reference frame, other
    bands might be reprojected if they use different pixel grid

    :param **kw: Any other parameter load_data accepts

    :return: Xarray dataset
    """
    from datacube import Datacube
    geobox = native_geobox(ds, measurements, basis)  # early exit via exception if no compatible grid exists
    if measurements is not None:
        mm = [ds.type.measurements[n] for n in measurements]
    else:
        mm = ds.type.measurements

    return Datacube.load_data(Datacube.group_datasets([ds], 'time'),
                              geobox,
                              measurements=mm, **kw)


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
    reproject_and_fuse([source], im, gbox, dst_nodata, resampling=resampling)
    return im


def write_gtiff(fname,
                pix,
                crs='epsg:3857',
                resolution=(10, -10),
                offset=(0.0, 0.0),
                nodata=None,
                overwrite=False,
                blocksize=None,
                gbox=None,
                **extra_rio_opts):
    """ Write ndarray to GeoTiff file.

    Geospatial info can be supplied either via
    - resolution, offset, crs
    or
    - gbox (takes precedence if supplied)
    """
    # pylint: disable=too-many-locals

    from affine import Affine
    import rasterio
    from pathlib import Path

    if pix.ndim == 2:
        h, w = pix.shape
        nbands = 1
        band = 1
    elif pix.ndim == 3:
        nbands, h, w = pix.shape
        band = tuple(i for i in range(1, nbands+1))
    else:
        raise ValueError('Need 2d or 3d ndarray on input')

    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.exists():
        if overwrite:
            fname.unlink()
        else:
            raise IOError("File exists")

    if gbox is not None:
        assert gbox.shape == (h, w)

        A = gbox.transform
        crs = str(gbox.crs)
    else:
        sx, sy = resolution
        tx, ty = offset

        A = Affine(sx, 0, tx,
                   0, sy, ty)

    rio_opts = dict(width=w,
                    height=h,
                    count=nbands,
                    dtype=pix.dtype.name,
                    crs=crs,
                    transform=A,
                    predictor=2,
                    compress='DEFLATE')

    if blocksize is not None:
        rio_opts.update(tiled=True,
                        blockxsize=min(blocksize, w),
                        blockysize=min(blocksize, h))

    if nodata is not None:
        rio_opts.update(nodata=nodata)

    rio_opts.update(extra_rio_opts)

    with rasterio.open(str(fname), 'w', driver='GTiff', **rio_opts) as dst:
        dst.write(pix, band)
        meta = dst.meta

    meta['gbox'] = gbox if gbox is not None else rio_geobox(meta)
    meta['path'] = fname
    return SimpleNamespace(**meta)


def dc_crs_from_rio(crs):
    from datacube.utils.geometry import CRS

    if crs.is_epsg_code:
        return CRS('epsg:{}'.format(crs.to_epsg()))
    return CRS(crs.wkt)


def rio_geobox(meta):
    """ Construct geobox from src.meta of opened rasterio dataset
    """
    if 'crs' not in meta or 'transform' not in meta:
        return None

    h, w = (meta['height'], meta['width'])
    crs = dc_crs_from_rio(meta['crs'])
    transform = meta['transform']

    return GeoBox(w, h, transform, crs)


def _fix_resampling(kw):
    r = kw.get('resampling', None)
    if isinstance(r, str):
        kw['resampling'] = resampling_s2rio(r)


def rio_slurp_reproject(fname, gbox, dtype=None, dst_nodata=None, **kw):
    """
    Read image with reprojection
    """
    import rasterio
    from rasterio.warp import reproject

    _fix_resampling(kw)

    with rasterio.open(str(fname), 'r') as src:
        if src.count == 1:
            shape = gbox.shape
            src_band = rasterio.band(src, 1)
        else:
            shape = (src.count, *gbox.shape)
            src_band = rasterio.band(src, tuple(range(1, src.count+1)))

        if dtype is None:
            dtype = src.dtypes[0]
        if dst_nodata is None:
            dst_nodata = src.nodata
        if dst_nodata is None:
            dst_nodata = 0

        pix = np.full(shape, dst_nodata, dtype=dtype)

        reproject(src_band, pix,
                  dst_nodata=dst_nodata,
                  dst_transform=gbox.transform,
                  dst_crs=str(gbox.crs),
                  **kw)

        meta = src.meta
        meta['src_gbox'] = rio_geobox(meta)
        meta['path'] = fname
        meta['gbox'] = gbox

        return pix, SimpleNamespace(**meta)


def rio_slurp_read(fname, out_shape=None, **kw):
    """
    Read whole image file using rasterio.

    :returns: ndarray (2d or 3d if multi-band), dict (rasterio meta)
    """
    import rasterio

    _fix_resampling(kw)

    if out_shape is not None:
        kw.update(out_shape=out_shape)

    with rasterio.open(str(fname), 'r') as src:
        data = src.read(1, **kw) if src.count == 1 else src.read(**kw)
        meta = src.meta
        src_gbox = rio_geobox(meta)

        same_gbox = out_shape is None or out_shape == src_gbox.shape
        gbox = src_gbox if same_gbox else gbx.zoom_to(src_gbox, out_shape)

        meta['src_gbox'] = src_gbox
        meta['gbox'] = gbox
        meta['path'] = fname
        return data, SimpleNamespace(**meta)


def rio_slurp(fname, *args, **kw):
    """
    Dispatches to either:

    rio_slurp_read(fname, out_shape, ..)
    rio_slurp_reproject(fname, gbox, ...)

    """
    if len(args) == 0:
        if 'gbox' in kw:
            return rio_slurp_reproject(fname, **kw)
        else:
            return rio_slurp_read(fname, **kw)

    if isinstance(args[0], GeoBox):
        return rio_slurp_reproject(fname, *args, **kw)
    else:
        return rio_slurp_read(fname, *args, **kw)
