

from __future__ import absolute_import, print_function

import click

import xarray as xr
import pandas as pd
import numpy as np
import rasterio

from dateutil.rrule import YEARLY
from datacube.api.geo_xarray import append_solar_day
from datacube.storage.masking import make_mask, mask_valid_data as invalid_data_mask
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents
from datacube import Datacube
from datacube.utils.dates import date_sequence

REQUIRED_MEASUREMENTS = ['red', 'green', 'blue']

DEFAULT_PRODUCTS = 'ls5_nbar_albers', 'ls7_nbar_albers', 'ls8_nbar_albers'

VALID_BIT = 8  # GA Landsat PQ Contiguity Bit


@click.command(name='sequencer')
@click.option('--app-config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location', callback=to_pathlib)
@click.option('--load-bounds-from', type=click.Path(exists=True, readable=True, dir_okay=False),
              help='Shapefile to calculate boundary coordinates from.')
@click.option('--start-date')
@click.option('--end-date')
@click.option('--stats-duration', help='eg. 1y, 3m')
@click.option('--step-size', help='eg. 1y, 3m')
@ui.global_cli_options
@ui.executor_cli_options
@ui.parsed_search_expressions
@ui.pass_index(app_name='agdc-moviemaker')
def sequencer(index, app_config, load_bounds_from, start_date, end_date, products, executor, expressions):
    products = products or DEFAULT_PRODUCTS
    _, config = next(read_documents(app_config))

    jobname = 'foo'
    job = {
        'bottom': -1619354.555,
        'left': 1188490.47,
        'top': -1578182.723,
        'right': 1213611.437
    },

    if load_bounds_from:
        crs, (left, bottom, right, top) = bounds_from_file(load_bounds_from)
    else:
        left, right, top, bottom = job['left'], job['right'], job['top'], job['bottom']

    results = []

    for filenum, date_range in enumerate(date_sequence(start_date, end_date, '1y', '1y')):
        task = dict(output_name=jobname, filenum=filenum, products=products, time=date_range, x=(left, right),
                    y=(top, bottom))
        if crs:
            task['crs'] = crs

        result_future = executor.submit(write_median, **task)
        results.append(result_future)

    for result in executor.as_completed(results):
        try:
            msg = executor.result(result)
            print(msg)
        except MemoryError as e:
            print(e)

    # Write subtitle file

    # Write video file

    print("Finished!")


def bounds_from_file(filename):
    with fiona.open(filename) as c:
        return c.crs_wkt, c.bounds


def write_median(filenum, output_name, **expression):
    year, median = load_and_compute(**expression)
    filename = output_name + "_{:03d}_{}.png".format(filenum, year)
    write_xarray_to_image(filename, median)
    return 'Wrote {}.'.format(filename)


def load_and_compute(products, **parsed_expressions):
    with Datacube() as dc:
        acq_range = parsed_expressions['time']
        print("Processing time range {}".format(acq_range))
        data = None
        #     datasets_by_season = defaultdict(list)
        datasets = []

        parsed_expressions['crs'] = 'EPSG:3577'

        for prodname in products:
            #         print("Loading data for {} during {}".format(prodname, acq_range), end="", flush=True)
            dataset = dc.load(product=prodname,
                              measurements=REQUIRED_MEASUREMENTS,
                              group_by='solar_day',
                              **parsed_expressions)
            if len(dataset) == 0:
                continue
            else:
                print("Found {} time slices of {} during {}.".format(len(dataset['time']), prodname, acq_range))

            pq = dc.load(product=prodname.replace('nbar', 'pq'),
                         group_by='solar_day',
                         fuse_func=pq_fuser,
                         **parsed_expressions)

            crs = dataset.crs
            dataset = dataset.where(dataset != -999)
            dataset.attrs['product'] = prodname
            dataset.attrs['crs'] = crs

            cloud_free = make_mask(pq.pixelquality, ga_good_pixel=True)
            dataset = dataset.where(cloud_free)

            if len(dataset) == 0:
                print("Nothing left after PQ masking")
                continue

            datasets.append(dataset)

        dataset = xr.concat(datasets, dim='solar_day')

        year = pd.Timestamp(dataset['solar_day'][0].data).year
    return year, dataset.median(dim='solar_day')


def pq_fuser(dest, src):
    valid_val = (1 << valid_bit)

    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)

    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)


def write_xarray_to_image(filename, dataset):
    img = np.stack([dataset[colour].data for colour in ['red', 'green', 'blue']])

    maxvalue = 3000
    nmask = np.isnan(img).any(axis=0)

    mask = (img > maxvalue).any(axis=0)
    # img[img == -999]
    img /= maxvalue
    img[..., mask] = 1.0
    img[..., nmask] = 1.0

    img *= 2 ** 16

    profile = {
        'driver': 'PNG',
        'height': len(dataset['y']),
        'width': len(dataset['x']),
        'count': 3,
        'dtype': 'uint16'
    }
    print("Writing file: {}".format(filename))
    with rasterio.open(filename, 'w', **profile) as dst:
        dst.write(img.astype('uint16'))


if __name__ == '__main__':
    sequencer()
