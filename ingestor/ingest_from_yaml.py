
import click
import os
import os.path
import yaml
import pathlib
from create_tiles import calc_target_names, create_tiles
from geotiff_to_netcdf import create_or_replace, MultiVariableNetCDF, SingleVariableNetCDF
from pprint import pprint


def read_yaml(filename):
    with open(filename) as f:
        data = yaml.load(f)
        return data


def get_input_files(input_path, data):
    """

    :type input_path: pathlib.Path
    :param data:
    :return:
    """

    assert input_path.is_dir()
    bands = sorted([band for band_num, band in data.image.bands.items()], key=lambda band: band.number)
    input_files = [band.path for band in bands]
    input_files = [input_path / filename for filename in input_files]

    return input_files


@click.command()
@click.option('--output-dir', '-o', default='.')
@click.option('--multi-variable', 'netcdf_class', flag_value=MultiVariableNetCDF, default=True)
@click.option('--single-variable', 'netcdf_class', flag_value=SingleVariableNetCDF)
@click.option('--read-yaml', 'input_type', flag_value='yaml', default=True)
@click.option('--read-dataset', 'input_type', flag_value='dataset')
@click.argument('path', type=click.Path(exists=True))
def main(path, output_dir, input_type, netcdf_class):
    os.chdir(output_dir)

    path = pathlib.Path(path)

    if input_type == 'yaml':
        dataset = read_yaml(path)
        pprint(dataset, indent=2)

        path = path.parent
        dataset = eodatasets.type.DatasetMetadata.from_dict(dataset)

    elif input_type == 'dataset':
        import eodatasets.drivers
        import eodatasets.type


        eodriver = eodatasets.drivers.EODSDriver()
        dataset = eodatasets.type.DatasetMetadata()
        eodriver.fill_metadata(dataset, path)


    input_files = get_input_files(path, dataset)
    basename = dataset.ga_label
    filename_format = 'combined_singlevar_{x}_{y}.nc'
    tile_options = {
        'output_format': 'GTiff',
        'create_options': ['COMPRESS=DEFLATE', 'ZLEVEL=1']
    }

    # Create Tiles
    create_tiles(input_files, output_dir, basename, tile_options)
    renames = calc_target_names('test.csv', filename_format, dataset)

    # Import into proper NetCDF files
    for geotiff, netcdf in renames:
        create_or_replace(geotiff, netcdf, dataset, netcdf_class=netcdf_class)





if __name__ == '__main__':
    try:
        from ipdb import launch_ipdb_on_exception
        with launch_ipdb_on_exception():
            main()
    except ImportError:
        main()
