"""
This app creates time series movies


"""

from __future__ import absolute_import, print_function

import click

import fiona
import xarray as xr
import pandas as pd
import numpy as np
import rasterio
from dateutil.parser import parse
from datetime import datetime, timedelta, time, date

from datacube.storage.masking import make_mask
from datacube.ui import click as ui
from datacube import Datacube
from datacube.utils.dates import date_sequence

COLOUR_BANDS = ('red', 'green', 'blue')

DEFAULT_PRODUCTS = ('ls5_nbar_albers', 'ls7_nbar_albers', 'ls8_nbar_albers')

VALID_BIT = 8  # GA Landsat PQ Contiguity Bit

DISPLAY_FORMAT = '%d %B %Y'
SRT_TIMEFMT = '%H:%M:%S,%f'
SRT_FORMAT = """
{i}
{start} --> {end}
{txt}"""
PATTERN = '\d\d\d\d'


def to_datetime(ctx, param, value):
    if value:
        return parse(value)
    else:
        return None


@click.command(name='moviemaker')
# @click.option('--app-config', '-c',
#               type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
#               help='configuration file location', callback=to_pathlib)
@click.option('--load-bounds-from', type=click.Path(exists=True, readable=True, dir_okay=False),
              help='Shapefile to calculate boundary coordinates from.')
@click.option('--start-date', callback=to_datetime)
@click.option('--end-date', callback=to_datetime)
@click.option('--stats-duration', default='1y', help='eg. 1y, 3m')
@click.option('--step-size', default='1y', help='eg. 1y, 3m')
@click.option('--bounds', nargs=4, help='LEFT, BOTTOM, RIGHT, TOP')
@click.option('--output-name')
@click.option('--time-incr', default=2, help='Time to display each image, in seconds')
@click.option('--product', multiple=True)
@ui.global_cli_options
@ui.executor_cli_options
# @ui.parsed_search_expressions
# @ui.pass_index(app_name='agdc-moviemaker')
def moviemaker(bounds, output_name, load_bounds_from, start_date, end_date, product, executor,
               step_size, stats_duration, time_incr):
    products = product or DEFAULT_PRODUCTS
    click.echo(products)

    if load_bounds_from:
        crs, (left, bottom, right, top) = bounds_from_file(load_bounds_from)
    elif bounds:
        left, bottom, right, top = bounds
    else:
        raise click.UsageError('Must specify one of --load-bounds-from or --bounds')

    tasks = []
    for filenum, date_range in enumerate(date_sequence(start_date, end_date, stats_duration, step_size)):
        task = dict(output_name=output_name, filenum=filenum, products=products, time=date_range, x=(left, right),
                    y=(top, bottom))
        if crs:
            task['crs'] = crs
        tasks.append(task)

    results = []
    for task in tasks:
        result_future = executor.submit(write_median, **task)
        results.append(result_future)

    filenames = []
    for result in executor.as_completed(results):
        try:
            filenames.append(executor.result(result))
        except MemoryError as e:
            print(e)

    # Write subtitle file
    write_subtitle_file(tasks, output_name="{}.srt".format(output_name), display_format=DISPLAY_FORMAT,
                        time_incr=time_incr)

    # Write video file
    write_video_file(tasks, filenames)

    click.echo("Finished!")


def bounds_from_file(filename):
    with fiona.open(filename) as c:
        return c.crs_wkt, c.bounds


def write_median(filenum, output_name, **expression):
    start_date, median = load_and_compute(**expression)
    filename = "{}_{:03d}_{:%Y-%m-%d}.png".format(output_name, filenum, start_date)
    write_xarray_to_image(filename, median)
    click.echo('Wrote {}.'.format(filename))
    return filename


def load_and_compute(products, **parsed_expressions):
    with Datacube() as dc:
        acq_range = parsed_expressions['time']
        start_date, _ = acq_range
        click.echo("Processing time range {}".format(acq_range))
        datasets = []

        parsed_expressions['crs'] = 'EPSG:3577'

        for prodname in products:
            dataset = dc.load(product=prodname,
                              measurements=COLOUR_BANDS,
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

    dataset = xr.concat(datasets, dim='time')

    return start_date, dataset.median(dim='time')


def pq_fuser(dest, src):
    valid_val = (1 << VALID_BIT)

    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)

    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)


def write_xarray_to_image(filename, dataset, dtype='uint16'):
    img = np.stack([dataset[colour].data for colour in COLOUR_BANDS])

    maxvalue = 3000
    nmask = np.isnan(img).any(axis=0)

    mask = (img > maxvalue).any(axis=0)
    img /= maxvalue
    img[..., mask] = 1.0
    img[..., nmask] = 1.0

    img *= 2 ** 16

    profile = {
        'driver': 'PNG',
        'height': len(dataset['y']),
        'width': len(dataset['x']),
        'count': 3,
        'dtype': dtype
    }
    click.echo("Writing file: {}".format(filename))
    with rasterio.open(filename, 'w', **profile) as dst:
        dst.write(img.astype(dtype))


# Write subtitle file
def write_subtitle_file(tasks, output_name, display_format, time_incr):
    if time_incr < 1.0:
        incr = timedelta(microseconds=time_incr * 1000000)
    else:
        incr = timedelta(seconds=time_incr)

    with open(output_name, mode='w') as output:
        start_time_vid = time(0, 0, 0, 0)
        for i, task in enumerate(tasks):
            end_time_vid = (datetime.combine(date.today(), start_time_vid) + incr).time()

            start_time_actual, _ = task['time']

            txt = start_time_actual.strftime(display_format)

            output.write(
                SRT_FORMAT.format(i=i, txt=txt,
                                  start=start_time_vid.strftime(SRT_TIMEFMT)[:-3],
                                  end=end_time_vid.strftime(SRT_TIMEFMT)[:-3]))
            start_time_vid = end_time_vid


# Write video file
def write_video_file(tasks, output_name):
    # resize images
    dims = `identify $filename | cut - d' ' - f3`
    newdims = `python - c
    "x, y = map(int, '$dims'.split('x')); scale = y / 1080; y = 1080; x = int(x/scale); x = x + 1 if x %2 == 1 else x; print('%sx%s' % (x,y))"
    `
    mogrify - geometry $newdims\! tmp / *.png
    ~ / ffmpeg - 3.1
    .2 - 64
    bit - static / ffmpeg - framerate
    1 /$timeincr - pattern_type
    glob - i
    tmp /\ *.png - c:v
    libx264 - pix_fmt
    yuv420p - r
    30 - vf
    subtitles = subs.srt:force_style = 'FontName=DejaVu Sans' $1
    _video.mp4

    # Run ffmpeg



if __name__ == '__main__':
    moviemaker()
