

from __future__ import absolute_import, print_function

import click
import numpy as np

from datacube.storage.masking import make_mask
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents
from datacube import Datacube

REQUIRED_MEASUREMENTS = ['red', 'green', 'blue']


@click.command(name='sequencer')
@click.option('--app-config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location', callback=to_pathlib)
@click.option('--load-bounds-from', type=click.Path(exists=True, readable=True, dir_okay=False),
              help='Shapefile to calculate boundary coordinates from.')
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-stats')
def sequencer(index, app_config, load_bounds_from, executor):
    _, config = next(read_documents(app_config))


    futures = [executor.submit(do_stats, task, config) for task in tasks]

    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


def load_data(products, **parsed_expressions):
    dc = Datacube()
    #     test_no = 0
    acq_range = parsed_expressions['time']
    print("Processing time range {}".format(acq_range))
    data = None
    #     datasets_by_season = defaultdict(list)
    datasets = []

    #     parsed_expressions['x'] = left, right
    #     parsed_expressions['y'] = top, bottom
    parsed_expressions['crs'] = 'EPSG:3577'

    for prodname in products:
        #         print("Loading data for {} during {}".format(prodname, acq_range), end="", flush=True)
        dataset = dc.load(product=prodname,
                          measurements=required_measurements,
                          **parsed_expressions)

        if len(dataset) == 0:
            continue
        else:
            print("Found {} time slices of {} during {}.".format(len(dataset['time']), prodname, acq_range))

        crs = dataset.crs
        dataset = dataset.where(dataset != -999)
        dataset.attrs['product'] = prodname
        dataset.attrs['crs'] = crs

        dataset = mask_data_with_pq(dc, dataset, prodname, parsed_expressions)

        if len(dataset) == 0:
            print("Nothing left after PQ masking")
            continue

        datasets.append(with_solar_day(dataset))

    # for season, dataset in by_season(data):  # Also merges to solar_day instead of time
    #             print("{} days of capture during {}".format(len(dataset['solar_day']), season))
    #             for i in range(len(dataset.solar_day)):
    #                 write_xarray_to_file('test%s%s.png' % (test_no, dataset.solar_day[i].item()), dataset.isel(solar_day=i).squeeze())
    #                 test_no += 1

    #             datasets_by_season[season].append(dataset)

    #     datasets_by_season = {season: xr.concat(season_datasets, dim='solar_day') for season, season_datasets in datasets_by_season.items()}

    dataset = xr.concat(datasets, dim='solar_day')

    year = pd.Timestamp(dataset['solar_day'][0].data).year
    return year, dataset.median(dim='solar_day')


#     to_return = []

#     for code, season_name in SEASONS:
#         if code not in datasets_by_season:
#             continue
#         ds = datasets_by_season[code].load()

#         year = pd.Timestamp(ds['solar_day'][0].data).year
#         median = ds.median(dim='solar_day')
#         to_return.append((season_name, year, median))

#     return to_return


def mask_data_with_pq(dc, data, prodname, parsed_expressions):
    mask_product_name = prodname.replace('nbar', 'pq')
    pq = dc.load(product=mask_product_name,
                 **parsed_expressions)
    if len(pq) > 0:
        cloud_free = make_mask(pq.pixelquality, ga_good_pixel=True)
        masked_data = data.where(cloud_free)
        masked_data.attrs = data.attrs
    else:
        print("No PQ Data found for {} {}".format(mask_product_name, parsed_expressions))
    return masked_data


def with_solar_day(year_data):
    append_solar_day(year_data)
    return year_data.groupby('solar_day').max(dim='time', keep_attrs=True)


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
