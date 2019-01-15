from typing import Optional, Dict, Any, Tuple
from datacube.model import Dataset
from datacube.utils.uris import uri_resolve, pick_uri


def _get_band_and_layer(b: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    """ Encode legacy logic for extracting band/layer:

        on input:
        band -- Int | Nothing
        layer -- Str | Int | Nothing

    Valid combinations are:
        band  layer  Output
    ---------------------------
          -     -    ( - ,  - )
          -    int   (int,  - )
         int    -    (int,  - )
         int   str   (int, str)
          -    str   ( - , str)

    """
    band = b.get('band')
    layer = b.get('layer')

    if band is None:
        if isinstance(layer, int):
            return (layer, None)
        if layer is None or isinstance(layer, str):
            return (None, layer)

        raise ValueError('Expect `layer` to be one of None,int,str but it is {}'.format(type(layer)))
    else:
        if not isinstance(band, int):
            raise ValueError('Expect `band` to be an integer (it is {})'.format(type(band)))
        if layer is not None and not isinstance(layer, str):
            raise ValueError('Expect `layer` to be one of None,str but it is {}'.format(type(layer)))

        return (band, layer)


def _extract_driver_data(ds: Dataset) -> Optional[Any]:
    dd = getattr(ds, 'driver_data', None)
    if dd is None:
        dd = getattr(ds, 's3_metadata', None)  # TODO: change CSIRO driver to populate `driver_data`
    return dd


class BandInfo:
    __slots__ = ('name',
                 'uri',
                 'band',
                 'layer',
                 'dtype',
                 'nodata',
                 'units',
                 'crs',
                 'transform',
                 'center_time',
                 'format',
                 'driver_data')

    def __init__(self,
                 ds: Dataset,
                 band: str,
                 uri_scheme: Optional[str] = None):
        mm = ds.measurements.get(band)
        mp = ds.type.measurements.get(band)

        if mm is None or mp is None:
            raise ValueError('No such band: {}'.format(band))

        if ds.uris is None:
            raise ValueError('No uris defined on a dataset')

        base_uri = pick_uri(ds.uris, uri_scheme)

        bint, layer = _get_band_and_layer(mm)

        self.name = band
        self.uri = uri_resolve(base_uri, mm.get('path'))
        self.band = bint
        self.layer = layer
        self.dtype = mp.dtype
        self.nodata = mp.nodata
        self.units = mp.units
        self.crs = ds.crs
        self.transform = ds.transform
        self.center_time = ds.center_time
        self.format = ds.format
        self.driver_data = _extract_driver_data(ds)
