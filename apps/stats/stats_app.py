"""
Calculates stats for small spatial area of time series LANDSAT data. It supports multiple sensors,
pq, interpolation methods, epoch and seasons

__author__ = 'u81051'
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import sys
import dask
import Queue
import threading
import time
import getopt
import numpy as np
from datetime import datetime
from collections import defaultdict
import logging
import rasterio
import datacube.api
import xarray as xr
from dateutil.relativedelta import relativedelta
from datacube.api.geo_xarray import append_solar_day, _get_spatial_dims
from datacube.storage.masking import make_mask
from dateutil.rrule import rrule, YEARLY
from enum import Enum

dask.set_options(get=dask.async.get_sync)
_log = logging.getLogger()

_log.setLevel(logging.DEBUG)
# dt = datetime(2015,2,5,01,15,31)
# dt = datetime(2010,2,22,23,33,54)
# with open('/g/data/u46/users/bxb547/bb/Darwin_all.txt', 'a') as f:

EXIT_FLAG = 0
QUEUE_LOCK = threading.Lock()
WORK_QUEUE = Queue.Queue(30)
MY_DATA = {}
NDV = -999


class MyStats(threading.Thread):
    """
     thread class to process result
    """

    def __init__(self, thread_id, acq_min, acq_max, season, dataset, band, stats, masks, sat, lon, lat, dc, q):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.acq_min = acq_min
        self.acq_max = acq_max
        self.season = season
        self.dataset = dataset
        self.band = band
        self.stats = stats
        self.masks = masks
        self.sat = sat
        self.lon = lon
        self.lat = lat
        self.dc = dc
        self.q = q
        print("myStats initialised ")

    def run(self):
        process_result(self.acq_min, self.acq_max, self.season)
        print("Exiting " + str(self.thread_id))


class Ls57Arg25Bands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    SHORT_WAVE_INFRARED_2 = 6


def process_result(acq_min, acq_max, q):
    while not EXIT_FLAG:
        QUEUE_LOCK.acquire()
        if not WORK_QUEUE.empty():
            data = q.get()
            _log.info("\t my data for epoch %s-%s is %s", str(acq_min), str(acq_max), data)
            print(" do your job with computed data")
            # data.plot()
            MY_DATA.update({str(acq_min): data})
            QUEUE_LOCK.release()
            print("%s processing %s" % (acq_min, data))
        else:
            QUEUE_LOCK.release()
        time.sleep(1)


def initialise_odata(dtype, y, x):
    shape = (y, x)
    nbar = np.empty(shape, dtype=dtype)
    nbar.fill(NDV)
    return nbar


def product_lookup(sat, dataset_type):
    """
    Finds product name from dataset type and sensor name
    :param sat: input dataset type and sensor name
    :param dataset_type: It can be pqa within nbar
    :return: product name like 'ls8_nbar_albers'
    """
    prod_list = [('ls5_nbar_albers', 'LANDSAT_5'), ('ls5_nbar_albers', 'nbar'),
                 ('ls7_nbar_albers', 'LANDSAT_7'), ('ls7_nbar_albers', 'nbar'),
                 ('ls8_nbar_albers', 'LANDSAT_8'), ('ls8_nbar_albers', 'nbar'),
                 ('ls5_nbart_albers', 'LANDSAT_5'), ('ls5_nbart_albers', 'nbart'),
                 ('ls7_nbart_albers', 'LANDSAT_7'), ('ls7_nbart_albers', 'nbart'),
                 ('ls8_nbart_albers', 'LANDSAT_8'), ('ls8_nbart_albers', 'nbart'),
                 ('ls5_pq_albers', 'LANDSAT_5'), ('ls5_pq_albers', 'pqa'),
                 ('ls7_pq_albers', 'LANDSAT_7'), ('ls7_pq_albers', 'pqa'),
                 ('ls8_pq_albers', 'LANDSAT_8'), ('ls8_pq_albers', 'pqa')]

    my_dict = defaultdict(list)
    for k, v in prod_list:
        my_dict[k].append(v)
    for k, v in my_dict.items():
        if sat in v[0] and dataset_type in v[1]:
            return k
    return None


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


def main(argv):
    season = 'WINTER'
    sat = 'LANDSAT_5'
    band = 'NDVI'
    epoch = 1
    start_str = '2009-01-01'
    end_str = '2011-01-01'
    masks = 'PQ_MASK_CLEAR_ELB'
    stats = 'PERCENTILE_25'
    dataset = 'nbar'
    interpolation = 'nearest'
    opts, args = getopt.getopt(argv, "hn:l:d:b:i:m:p:s:e:q:z:o:x",
                               ["lon=", "lat=", "dataset=", "band=", "stats=", "masks=", "epoch=", "start=", "end=",
                                "season=", "sat=", "odir=", "interpolation="])
    for opt, arg in opts:
        if opt == '-h':
            print('pass lon lat min max value like 130.378239-130.7823 -30.78276/-30.238')
            print(' For ex. python stats_jupyter.py -n 146.002/146.202 -l -34.970/-34.999 -i PERCENTILE_10 -s '
                  '2009-01-01 -e 2010-12-03 -z LANDSAT_5,LANDSAT_7 -q CALENDAR_YEAR -m PQ_MASK_CLEAR '
                  '-b NDVI -p 2 -d nbar')
            print('python stats_jupyter.py -o <output_dir> -n <lon> -l <lat> -d <dataset> -i <stats> '
                  '-m <masks> -p <epoch> -s <start_date> -e <end_date> -z <sat> -q <season> -x <interpolation>')
            sys.exit()
        elif opt in ("-n", "--lon"):
            lon = arg
        elif opt in ("-l", "--lat"):
            lat = arg
        elif opt in ("-d", "--dataset"):
            dataset = arg
        elif opt in ("-b", "--band"):
            band = arg
        elif opt in ("-i", "--stats"):
            stats = arg
        elif opt in ("-s", "--start"):
            start_str = arg
        elif opt in ("-e", "--end"):
            end_str = arg
        elif opt in ("-p", "--epoch"):
            epoch = arg
        elif opt in ("-m", "--masks"):
            masks = arg
        elif opt in ("-x", "--interpolation"):
            interpolation = arg
        elif opt in ("-z", "--sat"):
            sat = arg
        elif opt in ("-q", "--season"):
            season = arg
    print("arguments accepted start end lon lat epoch season interpolation stats masks band dataset sat",
          start_str, end_str, lon, lat, epoch, season, interpolation, stats, masks, band, dataset, sat)
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    epoch = int(epoch)
    threads = []
    thread_id = 1
    dc = datacube.Datacube(app='jupyter-test')
    for (acq_min, acq_max) in get_epochs(epoch, start, end):
        thread = MyStats(thread_id, acq_min, acq_max, season, dataset, band, stats, masks,
                         sat, lon, lat, dc, WORK_QUEUE)
        thread.start()
        threads.append(thread)
        print("thread_id", thread_id)
        thread_id += 1

    QUEUE_LOCK.acquire()
    for (acq_min, acq_max) in get_epochs(epoch, start, end):
        print("process starting for epoch period dates ", str(epoch), str(acq_min), str(acq_max))
        odata = load_data(dc, acq_min, acq_max, season, dataset, band, stats, masks,
                          lon, lat, sat, epoch)
        print("process completed for epoch", str(acq_min))
        WORK_QUEUE.put(odata)
    print("all process finished")
    QUEUE_LOCK.release()
    # Wait for queue to empty
    while not WORK_QUEUE.empty():
        pass
    global EXIT_FLAG
    # Notify threads it's time to exit
    EXIT_FLAG = 1

    # Wait for all threads to complete
    for t in threads:
        print(" waiting thread ", t)
        t.join()
    print("Exiting Main Thread")


def get_epochs(epoch, start, end):
    print("gettting epochs")
    for dt in rrule(YEARLY, interval=epoch, dtstart=start, until=end):
        acq_min = dt.date()
        acq_max = acq_min + relativedelta(years=epoch, days=-1)
        acq_min = max(start, acq_min)
        acq_max = min(end, acq_max)
        yield acq_min, acq_max


def get_derive_data(band, dataset, ls, data):
    ndvi = None
    blue = data.blue.where(data.blue != NDV)
    green = data.green.where(data.green != NDV)
    red = data.red.where(data.red != NDV)
    nir = data.nir.where(data.nir != NDV)
    sw1 = data.swir1.where(data.swir1 != NDV)
    sw2 = data.swir2.where(data.swir2 != NDV)
    if band == "NDFI":
        ndvi = (sw1 - nir) / (sw1 + nir)
    if band == "NDVI":
        ndvi = (nir - red) / (nir + red)
    if band == "NDWI":
        ndvi = (green - nir) / (green + nir)
    if band == "MNDWI":
        ndvi = (green - sw1) / (green + sw1)
    if band == "NBR":
        ndvi = (nir - sw2) / (nir + sw2)
    if dataset == "TCI":
        ndvi = calculate_tci(band, ls, blue, green, red, nir, sw1, sw2)
    return ndvi


def get_band_data(band, data):  # pylint: disable=too-many-branches
    band_data = None
    if band == "BLUE":
        band_data = data.blue.where(data.blue != NDV)
    if band == "GREEN":
        band_data = data.green.where(data.green != NDV)
    if band == "RED":
        band_data = data.red.where(data.red != NDV)
    if band == "NEAR_INFRARED":
        band_data = data.nir.where(data.nir != NDV)
    if band == "SHORT_WAVE_INFRARED_1":
        band_data = data.swir1.where(data.swir1 != NDV)
    if band == "SHORT_WAVE_INFRARED_2":
        band_data = data.swir2.where(data.swir2 != NDV)
    return band_data


def apply_mask(masks):
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


def load_data(dc, acq_min, acq_max, season, dataset, band, stats, masks, lon, lat, sat, epoch):
    prodname = None
    data = None
    lon_min = float(lon.split('/')[0])
    lon_max = float(lon.split('/')[1])
    lat_min = float(lat.split('/')[0])
    lat_max = float(lat.split('/')[1])
    datasets = []
    for st in sat.split(','):
        if st.find('5') > 0:
            prodname = 'ls5_nbar_albers'
        elif st.find('7') > 0:
            prodname = 'ls7_nbar_albers'
        elif st.find('8') > 0:
            prodname = 'ls8_nbar_albers'
        _log.info("\t loading nbar data found for (%f %f) satellite %s the date range  %s %s", lon_min, lat_min, st,
                  acq_min, acq_max)
        data = dc.load(product=prodname, x=(lon_min, lon_max), y=(lat_min, lat_max), time=(acq_min, acq_max),
                       measurements=['blue', 'green', 'red', 'nir', 'swir1', 'swir2'],
                       dask_chunks={'time': 1, 'y': 200, 'x': 200})
        if len(data) == 0:
            _log.info("\t No data found for (%f %f) in the date range  %s %s", lon_min, lat_min,
                      acq_min, acq_max)
            continue
        mask_clear = None
        if masks:
            prodname = product_lookup(st, 'pqa')
            pq = dc.load(product=prodname, x=(lon_min, lon_max), y=(lat_min, lat_max), time=(acq_min, acq_max),
                         dask_chunks={'time': 1, 'y': 200, 'x': 200})
            if len(pq) > 0:
                for mask in masks.split(','):
                    if mask == "PQ_MASK_CLEAR_ELB":
                        mask_clear = pq['pixelquality'] & 15871 == 15871
                    elif mask == "PQ_MASK_CLEAR":
                        mask_clear = pq['pixelquality'] & 16383 == 16383
                    else:
                        mask_clear = make_mask(pq, **apply_mask(masks))
                data = data.where(mask_clear)
            else:
                _log.info("\t No PQ data exists")
            append_solar_day(data, get_mean_longitude(data))
            data = data.groupby('solar_day').max(dim='time')
            season_dict = {'SUMMER': 'DJF', 'AUTUMN': 'MAM', 'WINTER': 'JJA', 'SPRING': 'SON',
                           'CALENDAR_YEAR': 'year', 'QTR_1': '1', 'QTR_2': '2',
                           'QTR_3': '3', 'QTR_4': '4'}
            if "QTR" in season:
                try:
                    data = data.isel(solar_day=data.groupby('solar_day.quarter').groups[int(season_dict[season])])
                except KeyError as e:
                    print(repr(e))
                    continue
            elif "CALENDAR" in season:
                if epoch == 1:
                    try:
                        year = int(str(data.groupby('solar_day.year').groups.keys()).strip('[]'))
                        data = data.isel(solar_day=data.groupby('solar_day.year').groups[year])
                    except KeyError as e:
                        print(repr(e))
                        continue

            else:
                print("Loading data for ", season, season_dict[season])
                try:
                    data = data.isel(solar_day=data.groupby('solar_day.season').groups[season_dict[season]])
                except KeyError as e:
                    print(repr(e))
                    continue

            print("Loaded data for ", st, acq_min, acq_max)
            if band in [t.name for t in Ls57Arg25Bands]:
                data = get_band_data(band, data)
            else:
                data = get_derive_data(band, dataset, st, data)
            datasets.append(data)

    xr.concat(datasets, dim='solar_day')

    dtype = np.float32
    if stats == "COUNT_OBSERVED" or band in [t.name for t in Ls57Arg25Bands]:
        dtype = np.int16
    if data.nbytes > 0:
        odata = initialise_odata(dtype, data.shape[1], data.shape[2])
        odata = do_compute(data, stats, odata)
        data = data.isel(solar_day=0).drop('solar_day')
        data.data = odata
        return data
    else:
        return None


class TasselCapIndex(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS FOURTH FIFTH SIXTH"

    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


class SatTwo(Enum):
    """
     Needs two satellites two use Tassel Cap Index properties as LS5 and LS7 are same
    """
    __order__ = "LANDSAT_5 LANDSAT_8"

    LANDSAT_5 = "LANDSAT_5"
    LANDSAT_8 = "LANDSAT_8"


class SixUniBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"
    BLUE = 'blue'
    GREEN = 'green'
    RED = 'red'
    NEAR_INFRARED = 'nir'
    SHORT_WAVE_INFRARED_1 = 'sw1'
    SHORT_WAVE_INFRARED_2 = 'sw2'


TCI_COEFF = {
    SatTwo.LANDSAT_5:
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
                SixUniBands.SHORT_WAVE_INFRARED_2: -0.2768},576

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
    SatTwo.LANDSAT_8:
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


def calculate_tci(band, satellite, blue, green, red, nir, sw1, sw2):
    all_bands = dict()
    bn = None
    masked_bands = dict()
    for t in SixUniBands:
        if t.name == "BLUE":
            bn = blue
        elif t.name == "GREEN":
            bn = green
        elif t.name == "RED":
            bn = red
        elif t.name == "NEAR_INFRARED":
            bn = nir
        elif t.name == "SHORT_WAVE_INFRARED_1":
            bn = sw1
        elif t.name == "SHORT_WAVE_INFRARED_2":
            bn = sw2
        all_bands.update({t: bn})
    for b in all_bands.keys():
        masked_bands[b] = all_bands[b].astype(np.float16)
        _log.info("mask band for %s is %s", b, masked_bands[b])
    tci = 0
    tci_cat = None
    for i in TasselCapIndex:
        if i.name == band:
            tci_cat = i
            break

    sat = satellite
    if sat == "LANDSAT_7":
        sat = "LANDSAT_5"
    for i in SatTwo:
        if i.name == sat:
            sat = i
    for b in SixUniBands:
        if b in TCI_COEFF[sat][tci_cat]:
            tci += masked_bands[b] * TCI_COEFF[sat][tci_cat][b]
            _log.info(" tci value for %s - %s of %s", b, tci, TCI_COEFF[sat][tci_cat])
    # tci = tci.filled(numpy.nan)
    _log.info(" TCI values calculated for %s %s - %s", sat, band, tci)
    return tci


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main(sys.argv[1:])
