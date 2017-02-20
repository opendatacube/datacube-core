import datacube

dc = datacube.Datacube(config='/home/547/gxr547/config/prodcube.conf')

query = {
   'lat': (-20.61, -20.66),
   'lon': (147.0, 147.05)
}

srtm_dem1sv1_0 = dc.load(product='srtm_dem1sv1_0', **query)

import numpy
import rasterio.io
import rasterio.warp
from datacube.utils import geometry
from datacube.storage.storage import _read_decimated, BandDataSource
from affine import Affine

src = numpy.arange(110*100, dtype='float64').reshape((110, 100))
dst = numpy.empty((200, 200))
transform = Affine.identity()
crs = geometry.CRS('EPSG:4326')
nodata = float('nan')

memfile = rasterio.io.MemoryFile()
with memfile.open(driver='GTiff',
                  width=src.shape[1],
                  height=src.shape[0],
                  count=1,
                  transform=Affine.identity(),
                  crs=str(crs),
                  nodata=nodata,
                  dtype=src.dtype) as thing:
    thing.write_band(1, src)

array_transform = Affine.translation(-1, 0)*Affine.scale(10, 10)
#array_transform = Affine.scale(10, 10)*Affine.translation(-1, -1)

with memfile.open() as ds:
    decim, write, decim_transform = _read_decimated(array_transform, BandDataSource(rasterio.band(ds, 1)), dst.shape)

rasterio.warp.reproject(src,
                    dst,
                    src_transform=transform,
                    dst_transform=transform*array_transform,
                    src_crs=str(crs),
                    dst_crs=str(crs),
                    src_nodata=float('nan'),
                    dst_nodata=float('nan'),
                    resampling=rasterio.warp.Resampling.nearest)
