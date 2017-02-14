from __future__ import absolute_import, division, print_function

try:
    from urlparse import parse_qs
except ImportError:
    from urllib.parse import parse_qs

# travis can only get earlier version of rasterio which doesn't have MemoryFile, so
# - tell pylint to ingnore inport error
# - catch ImportError so pytest doctest don't fall over
try:
    from rasterio.io import MemoryFile  # pylint: disable=import-error
except ImportError:
    MemoryFile = None

import numpy
import xarray
from affine import Affine
from datetime import datetime, timedelta

import datacube
from datacube.utils import geometry


INDEX_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>Map</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="shortcut icon" type="image/x-icon" href="docs/images/favicon.ico" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.0.2/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.0.2/dist/leaflet.js"></script>
</head>
<body>

<div id="mapid" style="width: 1200px; height: 800px;"></div>
<script>
    var mymap = L.map('mapid').setView([-35.28, 149.12], 12);

    L.tileLayer.wms(
        "{wms_url}",
        {{
            minZoom: 6,
            maxZoom: 19,
            layers: "ls8_nbar_rgb",
            format: 'image/png',
            transparent: true,
            attribution: "Teh Cube"
        }}
    ).addTo(mymap);
</script>
</body>
</html>
"""


GET_CAPS_TEMPLATE = """<?xml version='1.0' encoding="UTF-8" standalone="no" ?>
<!DOCTYPE WMT_MS_Capabilities SYSTEM "http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd"
 [
 <!ELEMENT VendorSpecificCapabilities EMPTY>
 ]>
<WMT_MS_Capabilities version="1.1.1"
        xmlns="http://www.opengis.net/wms"
        xmlns:py="http://genshi.edgewall.org/"
        xmlns:xlink="http://www.w3.org/1999/xlink">
<Service>
  <Name>Datacube WMS</Name>
  <Title>WMS server for Datacube</Title>
  <OnlineResource xlink:href="{location}"></OnlineResource>
</Service>
<Capability>
  <Request>
    <GetCapabilities>
      <Format>application/vnd.ogc.wms_xml</Format>
      <DCPType>
        <HTTP>
          <Get><OnlineResource xlink:href="{location}"></OnlineResource></Get>
        </HTTP>
      </DCPType>
    </GetCapabilities>
    <GetMap>
      <Format>image/png</Format>
      <DCPType>
        <HTTP>
          <Get><OnlineResource xlink:href="{location}"></OnlineResource></Get>
        </HTTP>
      </DCPType>
    </GetMap>
  </Request>
  <Exception>
    <Format>application/vnd.ogc.se_blank</Format>
  </Exception>
  <VendorSpecificCapabilities></VendorSpecificCapabilities>
  <UserDefinedSymbolization SupportSLD="1" UserLayer="0" UserStyle="1" RemoteWFS="0"/>
  <Layer>
    <Title>WMS server for Datacube</Title>
    <SRS>EPSG:3577</SRS>
    <SRS>EPSG:3857</SRS>
    <SRS>EPSG:4326</SRS>
    {layers}
  </Layer>
</Capability>
</WMT_MS_Capabilities>
"""

LAYER_TEMPLATE = """
<Layer>
  <Name>{name}</Name>
  <Title>{title}</Title>
  <Abstract>{abstract}</Abstract>
  {metadata}
</Layer>
"""

LAYER_SPEC = {
    'ls8_nbar_rgb': {
        'product': 'ls8_nbar_albers',
        'bands': ('red', 'green', 'blue'),
        'extents': geometry.box(100, -50, 160, 0, crs=geometry.CRS('EPSG:4326')),
        'time': {
            'start': datetime(2013, 1, 1),
            'end': datetime(2017, 1, 1),
            'period': timedelta(days=0)
        }
    },
    'ls8_l1t_rgb': {
        'product': 'ls8_l1t_scene',
        'bands': ('red', 'green', 'blue'),
        'extents': geometry.box(100, -50, 160, 0, crs=geometry.CRS('EPSG:4326')),
        'time': {
            'start': datetime(2013, 1, 1),
            'end': datetime(2017, 1, 1),
            'period': timedelta(days=0)
        }
    },
    'modis_mcd43a4_rgb': {
        'product': 'modis_mcd43a4_tile',
        'bands': ('Nadir_Reflectance_Band1', 'Nadir_Reflectance_Band4', 'Nadir_Reflectance_Band3'),
        'extents': geometry.box(100, -50, 160, 0, crs=geometry.CRS('EPSG:4326')),
        'time': {
            'start': datetime(2013, 1, 1),
            'end': datetime(2017, 1, 1),
            'period': timedelta(days=0)
        }
    }
}


class TileGenerator(object):
    def __init__(self, **kwargs):
        pass

    def datasets(self, index):
        pass

    def data(self, datasets):
        pass


class RGBTileGenerator(TileGenerator):
    def __init__(self, config, geobox, time, **kwargs):
        super(RGBTileGenerator, self).__init__(**kwargs)
        self._product = config['product']
        self._bands = config['bands']
        self._geobox = geobox
        self._time = time

    def datasets(self, index):
        return _get_datasets(index, self._geobox, self._product, self._time)

    def data(self, datasets):
        holder = numpy.empty(shape=tuple(), dtype=object)
        holder[()] = datasets
        sources = xarray.DataArray(holder)

        prod = datasets[0].type
        measurements = [self._set_resampling(prod.measurements[name]) for name in self._bands]
        with datacube.set_options(reproject_threads=1, fast_load=True):
            return datacube.Datacube.load_data(sources, self._geobox, measurements)

    def _set_resampling(self, measurement):
        mc = measurement.copy()
        # mc['resampling_method'] = 'cubic'
        return mc


def _get_datasets(index, geobox, product, time_):
    query = datacube.api.query.Query(product=product, geopolygon=geobox.extent, time=time_)
    datasets = index.datasets.search_eager(**query.search_terms)
    datasets.sort(key=lambda d: d.center_time)
    dataset_iter = iter(datasets)
    to_load = []
    for dataset in dataset_iter:
        if dataset.extent.to_crs(geobox.crs).intersects(geobox.extent):
            to_load.append(dataset)
            break
    else:
        return None

    geom = to_load[0].extent.to_crs(geobox.crs)
    for dataset in dataset_iter:
        if geom.contains(geobox.extent):
            break
        ds_extent = dataset.extent.to_crs(geobox.crs)
        if geom.contains(ds_extent):
            continue
        if ds_extent.intersects(geobox.extent):
            to_load.append(dataset)
            geom = geom.union(dataset.extent.to_crs(geobox.crs))
    return to_load


def application(environ, start_response):
    with datacube.Datacube(app="WMS") as dc:
        args = _parse_query(environ['QUERY_STRING'])

        if args.get('request') == 'GetMap':
            return get_map(dc, args, start_response)

        if args.get('request') == 'GetCapabilities':
            return get_capabilities(dc, args, environ, start_response)

        data = INDEX_TEMPLATE.format(wms_url=_script_url(environ)).encode('utf-8')

        start_response("200 OK", [
            ("Content-Type", "text/html"),
            ("Content-Length", str(len(data)))
        ])
        return iter([data])


def _parse_query(qs):
    return {key.lower(): (val[0] if len(val) == 1 else val) for key, val in parse_qs(qs).items()}


def _script_url(environ):
    return environ['wsgi.url_scheme']+'://'+environ['HTTP_HOST']+environ['SCRIPT_NAME']


def get_capabilities(dc, args, environ, start_response):
    layers = ""
    for name, layer in LAYER_SPEC.items():
        product = dc.index.products.get_by_name(layer['product'])
        if not product:
            continue
        layers += LAYER_TEMPLATE.format(name=name,
                                        title=name,
                                        abstract=product.definition['description'],
                                        metadata=get_layer_metadata(layer, product))


    data = GET_CAPS_TEMPLATE.format(location=_script_url(environ), layers=layers).encode('utf-8')
    start_response("200 OK", [
        ("Content-Type", "application/xml"),
        ("Content-Length", str(len(data)))
    ])
    return iter([data])


def get_layer_metadata(layer, product):
    metadata = """
<LatLonBoundingBox minx="100" miny="-50" maxx="160" maxy="0"></LatLonBoundingBox>
<BoundingBox CRS="EPSG:4326" minx="100" miny="-50" maxx="160" maxy="0"/>
<Dimension name="time" units="ISO8601"/>
<Extent name="time" default="2015-01-01">2013-01-01/2017-01-01/P8D</Extent>
    """
    return metadata


def get_map(dc, args, start_response):
    geobox = _get_geobox(args)
    time = args.get('time', '2015-01-01/2015-02-01').split('/')

    layer_config = LAYER_SPEC[args['layers']]
    tiler = RGBTileGenerator(layer_config, geobox, time)
    datasets = tiler.datasets(dc.index)
    data = tiler.data(datasets)

    body = _write_png(data)
    start_response("200 OK", [
        ("Content-Type", "image/png"),
        ("Content-Length", str(len(body)))
    ])
    return iter([body])


def _get_geobox(args):
    width = int(args['width'])
    height = int(args['height'])
    minx, miny, maxx, maxy = map(float, args['bbox'].split(','))
    crs = geometry.CRS(args['srs'])

    affine = Affine.translation(minx, miny) * Affine.scale((maxx - minx) / width, (maxy - miny) / height)
    return geometry.GeoBox(width, height, affine, crs)


def _write_png(data):
    width = data[data.crs.dimensions[1]].size
    height = data[data.crs.dimensions[0]].size

    with MemoryFile() as memfile:
        with memfile.open(driver='PNG',
                          width=width,
                          height=height,
                          count=len(data.data_vars),
                          transform=Affine.identity(),
                          nodata=0,
                          dtype='uint8') as thing:
            for idx, band in enumerate(data.data_vars, start=1):
                scaled = numpy.clip(data[band].values[::-1] / 12.0, 0, 255).astype('uint8')
                thing.write_band(idx, scaled)
        return memfile.read()


if __name__ == '__main__':
    from werkzeug.serving import run_simple  # pylint: disable=import-error
    run_simple('127.0.0.1', 8000, application, use_debugger=False, use_reloader=True)
