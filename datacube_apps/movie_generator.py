"""
This app creates time series movies


"""


import click

import fiona
import xarray as xr
import numpy as np
import rasterio
import subprocess
from glob import glob
from dateutil.parser import parse
from datetime import datetime, timedelta, time, date

from datacube.storage.masking import make_mask
from datacube.ui import click as ui
from datacube import Datacube
from datacube.utils.dates import date_sequence
from datacube.helpers import ga_pq_fuser

DEFAULT_MEASUREMENTS = ['red', 'green', 'blue']

DEFAULT_PRODUCTS = ['ls5_nbar_albers', 'ls7_nbar_albers', 'ls8_nbar_albers']
DEFAULT_CRS = 'EPSG:3577'
FFMPEG_PATH = 'ffmpeg'
VALID_BIT = 8  # GA Landsat PQ Contiguity Bit

SUBTITLE_FORMAT = '%d %B %Y'
SRT_TIMEFMT = '%H:%M:%S,%f'
SRT_FORMAT = """
{i}
{start} --> {end}
{txt}"""


def to_datetime(ctx, param, value):
    if value:
        return parse(value)
    else:
        return None


# pylint: disable=too-many-arguments, too-many-locals
@click.command(name='moviemaker')
@click.option('--load-bounds-from', type=click.Path(exists=True, readable=True, dir_okay=False),
              help='Shapefile to calculate boundary coordinates from.')
@click.option('--start-date', callback=to_datetime, help='YYYY-MM-DD')
@click.option('--end-date', callback=to_datetime, help='YYYY-MM-DD')
@click.option('--stats-duration', default='1y', help='eg. 1y, 3m')
@click.option('--step-size', default='1y', help='eg. 1y, 3m')
@click.option('--bounds', nargs=4, help='LEFT, BOTTOM, RIGHT, TOP')
@click.option('--base-output-name', default='output', help="Base name to use for images and video. Eg.  "
                                                           "--base-output-name stromlo will produce "
                                                           "stromlo_001_*.png and stromlo.mp4")
@click.option('--time-incr', default=2, help='Time to display each image, in seconds')
@click.option('--product', multiple=True, default=DEFAULT_PRODUCTS)
@click.option('--measurement', '-m', multiple=True, default=DEFAULT_MEASUREMENTS)
@click.option('--ffmpeg-path', default=FFMPEG_PATH, help='Path to ffmpeg executable')
@click.option('--crs', default=DEFAULT_CRS, help='Used if specifying --bounds. eg. EPSG:3577. ')
@ui.global_cli_options
@ui.executor_cli_options
def main(bounds, base_output_name, load_bounds_from, start_date, end_date, product, measurement, executor,
         step_size, stats_duration, time_incr, ffmpeg_path, crs):
    """
    Create an mp4 movie file based on datacube data

    Use only clear pixels, and mosaic over time to produce full frames.

    Can combine products, specify multiple --product

    """
    if load_bounds_from:
        crs, (left, bottom, right, top) = bounds_from_file(load_bounds_from)
    elif bounds:
        left, bottom, right, top = bounds
    else:
        raise click.UsageError('Must specify one of --load-bounds-from or --bounds')

    tasks = []
    for filenum, date_range in enumerate(date_sequence(start_date, end_date, stats_duration, step_size), start=1):
        filename = "{}_{:03d}_{:%Y-%m-%d}.png".format(base_output_name, filenum, start_date)
        task = dict(filename=filename, products=product, time=date_range, x=(left, right),
                    y=(top, bottom), crs=crs, measurements=measurement)
        tasks.append(task)

    results = []
    for task in tasks:
        result_future = executor.submit(write_mosaic_to_file, **task)
        results.append(result_future)

    filenames = []
    for result in executor.as_completed(results):
        filenames.append(executor.result(result))

    # Write subtitle file
    subtitle_filename = "{}.srt".format(base_output_name)
    write_subtitle_file(tasks, subtitle_filename=subtitle_filename, display_format=SUBTITLE_FORMAT,
                        time_incr=time_incr)

    # Write video file
    filenames_pattern = '%s*.png' % base_output_name
    video_filename = "{}.mp4".format(base_output_name)
    write_video_file(filenames_pattern, video_filename, subtitle_filename, time_incr=time_incr, ffmpeg_path=ffmpeg_path)

    click.echo("Finished!")


def bounds_from_file(filename):
    with fiona.open(filename) as c:
        return c.crs_wkt, c.bounds


def write_mosaic_to_file(filename, **expression):
    image_data = compute_mosaic(**expression)
    write_xarray_to_image(filename, image_data)
    click.echo('Wrote {}.'.format(filename))
    return filename


def compute_mosaic(products, measurements, **parsed_expressions):
    with Datacube() as dc:
        acq_range = parsed_expressions['time']
        click.echo("Processing time range {}".format(acq_range))
        datasets = []

        for prodname in products:
            dataset = dc.load(product=prodname,
                              measurements=measurements,
                              group_by='solar_day',
                              **parsed_expressions)
            if len(dataset) == 0:
                continue
            else:
                click.echo("Found {} time slices of {} during {}.".format(len(dataset['time']), prodname, acq_range))

            pq = dc.load(product=prodname.replace('nbar', 'pq'),
                         group_by='solar_day',
                         fuse_func=ga_pq_fuser,
                         **parsed_expressions)

            if len(pq) == 0:
                click.echo('No PQ found, skipping')
                continue

            crs = dataset.attrs['crs']
            dataset = dataset.where(dataset != -999)
            dataset.attrs['product'] = prodname
            dataset.attrs['crs'] = crs

            cloud_free = make_mask(pq.pixelquality, ga_good_pixel=True)
            dataset = dataset.where(cloud_free)

            if len(dataset) == 0:
                click.echo("Nothing left after PQ masking")
                continue

            datasets.append(dataset)

    dataset = xr.concat(datasets, dim='time')

    return dataset.median(dim='time')


def write_xarray_to_image(filename, dataset, dtype='uint16'):
    img = np.stack([dataset[colour].data for colour in DEFAULT_MEASUREMENTS])

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


def write_subtitle_file(tasks, subtitle_filename, display_format, time_incr):
    if time_incr < 1.0:
        incr = timedelta(microseconds=time_incr * 1000000)
    else:
        incr = timedelta(seconds=time_incr)

    with open(subtitle_filename, mode='w') as output:
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


def write_video_file(filename_pattern, video_filename, subtitle_filename, time_incr, ffmpeg_path):
    resize_images(filename_pattern)

    # Run ffmpeg
    movie_cmd = [ffmpeg_path, '-framerate', '1/%s' % time_incr, '-pattern_type', 'glob',
                 '-i', filename_pattern, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-r', '30',
                 '-vf', "subtitles='%s':force_style='FontName=DejaVu Sans'" % subtitle_filename, video_filename]

    subprocess.check_call(movie_cmd)


def resize_images(filename_pattern):
    """
    Resize images files in place to a safe size for movie generation

    - Maximum height of 1080
    - Ensure dimensions are divisible by 2.

    Uses the ImageMagick mogrify command.
    """
    sample_file = glob(filename_pattern)[0]
    width, height = subprocess.check_output(['identify', sample_file]).decode('ascii').split()[2].split('x')
    x, y = int(width), int(height)
    if y > 1080:
        scale = y / 1080
        y = 1080
        x = int(x / scale)
    x = x + 1 if x % 2 == 1 else x
    y = y + 1 if y % 2 == 1 else y
    newdims = '%sx%s!' % (x, y)
    resize_cmd = ['mogrify', '-geometry', newdims, filename_pattern]
    subprocess.check_call(resize_cmd)


if __name__ == '__main__':
    main()
