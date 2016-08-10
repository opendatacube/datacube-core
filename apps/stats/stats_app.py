"""
Calculates stats for small spatial area of time series LANDSAT data. It supports multiple sensors,
pq, interpolation methods, epoch and seasons

__author__ = 'u81051'
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import sys
import click
import functools
import numpy as np
from datetime import datetime
import logging
import rasterio
import datacube.api
from datacube.ui.expression import parse_expressions

from dateutil.relativedelta import relativedelta
from datacube.api.geo_xarray import append_solar_day, _get_spatial_dims
from datacube.storage.masking import make_mask
from dateutil.rrule import rrule, YEARLY
from datacube.ui import click as ui
from enum import Enum

_log = logging.getLogger()


MY_DATA = {}
NDV = -999
SEASONAL_OPTIONS = {'SUMMER': 'DJF', 'AUTUMN': 'MAM', 'WINTER': 'JJA', 'SPRING': 'SON',
                    'CALENDAR_YEAR': 'year', 'QTR_1': '1', 'QTR_2': '2',
                    'QTR_3': '3', 'QTR_4': '4'}


class Ls57Arg25Bands(Enum):
    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    SHORT_WAVE_INFRARED_2 = 6


def initialise_odata(dtype, y, x):
    odata = np.empty(shape=(y, x), dtype=dtype)
    odata.fill(NDV)
    return odata


def get_mean_longitude(cell_dataset):
    x, y = _get_spatial_dims(cell_dataset)
    mean_lat = float(cell_dataset[x][0] + cell_dataset[x][-1]) / 2.
    mean_lon = float(cell_dataset[y][0] + cell_dataset[y][-1]) / 2.
    bounds = {'left': mean_lon, 'right': mean_lon, 'top': mean_lat, 'bottom': mean_lat}
    input_crs = cell_dataset.crs.wkt
    left, bottom, right, top = rasterio.warp.transform_bounds(input_crs, 'EPSG:4326', **bounds)
    return left


def do_compute(data, stats, odata):

    _log.info("doing computations for %s on  %s of on odata shape %s",
              stats, datetime.now(), odata.shape)
    ndv = np.nan
    _log.info("\t data shape %s", data.shape)

    stack = data.isel(x=slice(0, data.shape[2]), y=slice(0, data.shape[1])).load().data
    _log.info("data stack is  %s", stack)
    stack_stat = None
    if stats == "MIN":
        stack_stat = np.nanmin(stack, axis=0)
    elif stats == "MAX":
        stack_stat = np.nanmax(stack, axis=0)
    elif stats == "MEAN":
        stack_stat = np.nanmean(stack, axis=0)
    elif stats == "GEOMEDIAN":  # Not implemented
        tran_data = np.transpose(stack)
        _log.info("\t shape of data array to pass %s", np.shape(tran_data))
        # stack_stat = geomedian(tran_data, 1e-3, maxiters=20)
    elif stats == "MEDIAN":
        stack_stat = np.nanmedian(stack, axis=0)
    elif stats == "VARIANCE":
        stack_stat = np.var(stack, axis=0).filled(ndv)
    elif stats == "STANDARD_DEVIATION":
        stack_stat = np.std(stack, axis=0).filled(ndv)
    elif stats == "COUNT_OBSERVED":
        stack_stat = np.ma.masked_invalid(stack, copy=False).count(axis=0)
    elif 'PERCENTILE' in stats.split('_'):
        percent = int(str(stats).split('_')[1])
        _log.info("\tcalculating percentile %d", percent)
        stack_stat = np.nanpercentile(a=stack, q=percent, axis=0, interpolation='nearest')

    odata[0:data.shape[1], 0:data.shape[2]] = stack_stat
    _log.info("stats finished for data  %s", odata)
    return odata


def parse_date(context, param, value):  # TODO: Convert to a click.ParamType subclass
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except TypeError:
        return None


required_option = functools.partial(click.option, required=True)


@click.command(name='stats')
@click.argument('expression', nargs=-1)
@required_option('--season', type=click.Choice(SEASONAL_OPTIONS.keys()))
@required_option('--band')
@required_option('--stats')
@required_option('--epoch')
@required_option('--start', callback=parse_date)
@required_option('--end', callback=parse_date)
@click.option('--masks', multiple=True)
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-stats-app')
def main(index, season, band, stats, start, end, epoch, masks, expression, executor):
    tasks = create_stats_tasks(band, end, epoch, masks, season, start, stats, expression)

    results = execute_tasks(executor, index, tasks)

    process_results(executor, results)


def create_stats_tasks(band, end, epoch, masks, season, start, stats, expression):
    tasks = []
    for acq_min, acq_max in get_epochs(epoch, start, end):
        task = dict(acq_min=acq_min, acq_max=acq_max, season=season, band=band,
                    stats=stats, masks=masks, epoch=epoch, expression=expression)
        tasks.append(task)
    return tasks


def execute_tasks(executor, index, tasks):
    results = []
    dc = datacube.Datacube(index=index)
    for task in tasks:
        result_future = executor.submit(load_data, dc=dc, **task)
        results.append(result_future)
    return results


def process_results(executor, results):
    for result in executor.as_completed(results):
        acq_min, data = executor.result(result)
        MY_DATA[acq_min] = data


# def old_main(argv):
#     season = 'WINTER'
#     sat = 'LANDSAT_5'
#     band = 'NDVI'
#     epoch = 1
#     start_str = '2009-01-01'
#     end_str = '2011-01-01'
#     masks = 'PQ_MASK_CLEAR_ELB'
#     stats = 'PERCENTILE_25'
#     dataset = 'nbar'
#     interpolation = 'nearest'
#     opts, args = getopt.getopt(argv, "hn:l:d:b:i:m:p:s:e:work_queue:z:o:x",
#                                ["lon=", "lat=", "dataset=", "band=", "stats=", "masks=", "epoch=", "start=", "end=",
#                                 "season=", "sat=", "odir=", "interpolation="])
#     for opt, arg in opts:
#         if opt == '-h':
#             print('pass lon lat min max value like 130.378239-130.7823 -30.78276/-30.238')
#             print(' For ex. python stats_jupyter.py -n 146.002/146.202 -l -34.970/-34.999 -i PERCENTILE_10 -s '
#                   '2009-01-01 -e 2010-12-03 -z LANDSAT_5,LANDSAT_7 -work_queue CALENDAR_YEAR -m PQ_MASK_CLEAR '
#                   '-b NDVI -p 2 -d nbar')
#             print('python stats_jupyter.py -o <output_dir> -n <lon> -l <lat> -d <dataset> -i <stats> '
#                   '-m <masks> -p <epoch> -s <start_date> -e <end_date> '
#                   '-z <sat> -work_queue <season> -x <interpolation>')
#             sys.exit()


def get_epochs(epoch, start, end):
    for dt in rrule(YEARLY, interval=epoch, dtstart=start, until=end):
        acq_min = dt.date()
        acq_max = acq_min + relativedelta(years=epoch, days=-1)
        acq_min = max(start, acq_min)
        acq_max = min(end, acq_max)
        yield acq_min, acq_max


def derive_data(band, dataset, prodname, data):
    blue = data.blue.where(data.blue != NDV)
    green = data.green.where(data.green != NDV)
    red = data.red.where(data.red != NDV)
    nir = data.nir.where(data.nir != NDV)
    sw1 = data.swir1.where(data.swir1 != NDV)
    sw2 = data.swir2.where(data.swir2 != NDV)
    if band == "NDFI":
        return (sw1 - nir) / (sw1 + nir)
    if band == "NDVI":
        return (nir - red) / (nir + red)
    if band == "NDWI":
        return (green - nir) / (green + nir)
    if band == "MNDWI":
        return (green - sw1) / (green + sw1)
    if band == "NBR":
        return (nir - sw2) / (nir + sw2)
    if dataset == "TCI":
        return calculate_tci(prodname, blue=blue, green=green, red=red, nir=nir, sw1=sw1, sw2=sw2)


def get_band_data(band, data):
    if band == "BLUE":
        return data.blue.where(data.blue != NDV)
    if band == "GREEN":
        return data.green.where(data.green != NDV)
    if band == "RED":
        return data.red.where(data.red != NDV)
    if band == "NEAR_INFRARED":
        return data.nir.where(data.nir != NDV)
    if band == "SHORT_WAVE_INFRARED_1":
        return data.swir1.where(data.swir1 != NDV)
    if band == "SHORT_WAVE_INFRARED_2":
        return data.swir2.where(data.swir2 != NDV)


def create_mask_def(masks):
    ga_pixel_bit = {name: True for name in
                    ('swir2_saturated',
                     'red_saturated',
                     'blue_saturated',
                     'nir_saturated',
                     'green_saturated',
                     'tir_saturated',
                     'swir1_saturated')}
    ga_pixel_bit.update(dict(contiguous=False, land_sea='land', cloud_shadow_acca='no_cloud_shadow',
                             cloud_acca='no_cloud', cloud_fmask='no_cloud',
                             cloud_shadow_fmask='no_cloud_shadow'))

    for mask in masks.split(','):
        if mask == "PQ_MASK_CONTIGUITY":
            ga_pixel_bit.update(dict(contiguous=True))
        if mask == "PQ_MASK_CLOUD_FMASK":
            ga_pixel_bit.update(dict(cloud_fmask='no_cloud'))
        if mask == "PQ_MASK_CLOUD_ACCA":
            ga_pixel_bit.update(dict(cloud_acca='no_cloud_shadow'))
        if mask == "PQ_MASK_CLOUD_SHADOW_ACCA":
            ga_pixel_bit.update(dict(cloud_shadow_acca='no_cloud_shadow'))
        if mask == "PQ_MASK_SATURATION":
            ga_pixel_bit.update(dict(blue_saturated=False, green_saturated=False, red_saturated=False,
                                     nir_saturated=False, swir1_saturated=False, tir_saturated=False,
                                     swir2_saturated=False))
        if mask == "PQ_MASK_SATURATION_OPTICAL":
            ga_pixel_bit.update(dict(blue_saturated=False, green_saturated=False, red_saturated=False,
                                     nir_saturated=False, swir1_saturated=False, swir2_saturated=False))
        if mask == "PQ_MASK_SATURATION_THERMAL":
            ga_pixel_bit.update(dict(tir_saturated=False))
        _log.info("applying bit mask %s on %s ", mask, ga_pixel_bit)

    return ga_pixel_bit


def load_data(dc, products, acq_min, acq_max, season, band, stats, masks, epoch, expression):
    data = None
    datasets = []
    for prodname in products:
        _log.info("\t loading data found for product %s the date range %s %s expression %s", prodname,
                  acq_min, acq_max, expression)
        data = dc.load(product=prodname,
                       measurements=['blue', 'green', 'red', 'nir', 'swir1', 'swir2'],
                       dask_chunks={'time': 1, 'y': 200, 'x': 200},
                       **parse_expressions(*expression))
        if len(data) == 0:
            _log.info("\t No data found")
            continue
        if masks:
            data = mask_data_with_pq(dc, data, prodname, acq_max, acq_min, expression, masks)

            data = group_by_season_epoch(data, epoch, season)

            print("Loaded data for ", prodname, acq_min, acq_max)

            if band in [t.name for t in Ls57Arg25Bands]:
                data = get_band_data(band, data)
            else:
                data = derive_data(band, "dataset", prodname, data)
            datasets.append(data)

    # xr.concat(datasets, dim='solar_day')

    dtype = determine_output_dtype(band, stats)
    if data.nbytes > 0:
        odata = initialise_odata(dtype, data.shape[1], data.shape[2])
        odata = do_compute(data, stats, odata)
        data = data.isel(solar_day=0).drop('solar_day')
        data.data = odata
        return data
    else:
        return None


def determine_output_dtype(band, stats):
    dtype = np.float32
    if stats == "COUNT_OBSERVED" or band in [t.name for t in Ls57Arg25Bands]:
        dtype = np.int16
    return dtype


def group_by_season_epoch(data, epoch, season):
    append_solar_day(data, get_mean_longitude(data))
    data = data.groupby('solar_day').max(dim='time')
    if "QTR" in season:
        data = data.isel(solar_day=data.groupby('solar_day.quarter').groups[int(SEASONAL_OPTIONS[season])])
    elif "CALENDAR" in season:
        if epoch == 1:
            year = int(str(data.groupby('solar_day.year').groups.keys()).strip('[]'))
            data = data.isel(solar_day=data.groupby('solar_day.year').groups[year])
    else:
        print("Loading data for ", season, SEASONAL_OPTIONS[season])
        data = data.isel(solar_day=data.groupby('solar_day.season').groups[SEASONAL_OPTIONS[season]])
    return data


def mask_data_with_pq(dc, data, prodname, acq_max, acq_min, expression, masks):
    mask_clear = None
    pq_prodname = prodname.replace('nbar', 'pqa')
    pq = dc.load(product=pq_prodname, time=(acq_min, acq_max),
                 dask_chunks={'time': 1, 'y': 200, 'x': 200},
                 **parse_expressions(*expression))
    if len(pq) > 0:
        for mask in masks.split(','):
            if mask == "PQ_MASK_CLEAR_ELB":
                mask_clear = pq['pixelquality'] & 15871 == 15871
            elif mask == "PQ_MASK_CLEAR":
                mask_clear = pq['pixelquality'] & 16383 == 16383
            else:
                mask_clear = make_mask(pq, **create_mask_def(masks))
        data = data.where(mask_clear)
    else:
        _log.info("\t No PQ data exists")
    return data


class TasselCapIndex(Enum):
    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


class Landsats(Enum):
    """
     Needs two satellites two use Tassel Cap Index properties as LS5 and LS7 are same
    """

    LANDSAT_5 = "ls5_nbar_albers"
    LANDSAT_7 = "ls7_nbar_albers"
    LANDSAT_8 = "ls8_nbar_albers"


class SixUniBands(Enum):
    BLUE = 'blue'
    GREEN = 'green'
    RED = 'red'
    NEAR_INFRARED = 'nir'
    SHORT_WAVE_INFRARED_1 = 'sw1'
    SHORT_WAVE_INFRARED_2 = 'sw2'


TCI_COEFF = {
    Landsats.LANDSAT_5:
        {
            TasselCapIndex.BRIGHTNESS: {
                SixUniBands.BLUE: 0.3037,
                SixUniBands.GREEN: 0.2793,
                SixUniBands.RED: 0.4743,
                SixUniBands.NEAR_INFRARED: 0.5585,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.5082,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.1863},

            TasselCapIndex.GREENNESS: {
                SixUniBands.BLUE: -0.2848,
                SixUniBands.GREEN: -0.2435,
                SixUniBands.RED: -0.5436,
                SixUniBands.NEAR_INFRARED: 0.7243,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.0840,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.1800},

            TasselCapIndex.WETNESS: {
                SixUniBands.BLUE: 0.1509,
                SixUniBands.GREEN: 0.1973,
                SixUniBands.RED: 0.3279,
                SixUniBands.NEAR_INFRARED: 0.3406,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.7112,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.4572},

            TasselCapIndex.FOURTH: {
                SixUniBands.BLUE: -0.8242,
                SixUniBands.GREEN: 0.0849,
                SixUniBands.RED: 0.4392,
                SixUniBands.NEAR_INFRARED: -0.0580,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.2012,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.2768},

            TasselCapIndex.FIFTH: {
                SixUniBands.BLUE: -0.3280,
                SixUniBands.GREEN: 0.0549,
                SixUniBands.RED: 0.1075,
                SixUniBands.NEAR_INFRARED: 0.1855,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.4357,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.8085},

            TasselCapIndex.SIXTH: {
                SixUniBands.BLUE: 0.1084,
                SixUniBands.GREEN: -0.9022,
                SixUniBands.RED: 0.4120,
                SixUniBands.NEAR_INFRARED: 0.0573,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.0251,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.0238}
        },
    Landsats.LANDSAT_8:
        {
            TasselCapIndex.BRIGHTNESS: {
                SixUniBands.BLUE: 0.3029,
                SixUniBands.GREEN: 0.2786,
                SixUniBands.RED: 0.4733,
                SixUniBands.NEAR_INFRARED: 0.5599,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.508,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.1872},

            TasselCapIndex.GREENNESS: {
                SixUniBands.BLUE: -0.2941,
                SixUniBands.GREEN: -0.2430,
                SixUniBands.RED: -0.5424,
                SixUniBands.NEAR_INFRARED: 0.7276,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.0713,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.1608},

            TasselCapIndex.WETNESS: {
                SixUniBands.BLUE: 0.1511,
                SixUniBands.GREEN: 0.1973,
                SixUniBands.RED: 0.3283,
                SixUniBands.NEAR_INFRARED: 0.3407,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.7117,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.4559},

            TasselCapIndex.FOURTH: {
                SixUniBands.BLUE: -0.8239,
                SixUniBands.GREEN: 0.0849,
                SixUniBands.RED: 0.4396,
                SixUniBands.NEAR_INFRARED: -0.058,
                SixUniBands.SHORT_WAVE_INFRARED_1: 0.2013,
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.2773},

            TasselCapIndex.FIFTH: {
                SixUniBands.BLUE: -0.3294,
                SixUniBands.GREEN: 0.0557,
                SixUniBands.RED: 0.1056,
                SixUniBands.NEAR_INFRARED: 0.1855,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.4349,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.8085},

            TasselCapIndex.SIXTH: {
                SixUniBands.BLUE: 0.1079,
                SixUniBands.GREEN: -0.9023,
                SixUniBands.RED: 0.4119,
                SixUniBands.NEAR_INFRARED: 0.0575,
                SixUniBands.SHORT_WAVE_INFRARED_1: -0.0259,
                SixUniBands.SHORT_WAVE_INFRARED_2: 0.0252}
        }
}

TCI_COEFF[Landsats.LANDSAT_7] = TCI_COEFF[Landsats.LANDSAT_5]


def calculate_tci(prodname, **bands):
    bands_masked = {SixUniBands(colour): data.astype(np.float16) for colour, data in bands.items()}

    coefficients = TCI_COEFF[Landsats(prodname)]

    tci = 0

    for b in SixUniBands:
        if b in coefficients:
            tci += bands_masked[b] * coefficients[b]

    tci = tci.filled(NDV)
    return tci


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main(sys.argv[1:])
