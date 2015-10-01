
import click
import os
import os.path
import yaml
from create_tiles import calc_target_names, create_tiles
from geotiff_to_netcdf import create_or_replace, MultiVariableNetCDF, SingleVariableNetCDF
from pprint import pprint


def read_yaml(filename):
    with open(filename) as f:
        data = yaml.load(f)
        return data


def get_input_files(input_file, data):
    bands = sorted([band for band_num, band in data['image']['bands'].items()], key=lambda band: band['number'])
    input_files = [band['path'] for band in bands]
    base_input_directory = os.path.dirname(input_file)
    input_files = [os.path.join(base_input_directory, filename) for filename in input_files]

    return input_files


@click.command()
@click.option('--output-dir', '-o', default='.')
@click.option('--multi-variable', 'netcdf_class', flag_value=MultiVariableNetCDF, default=True)
@click.option('--single-variable', 'netcdf_class', flag_value=SingleVariableNetCDF)
@click.argument('yaml_file', type=click.Path(exists=True))
def main(yaml_file, output_dir, netcdf_class):
    os.chdir(output_dir)

    data = read_yaml(yaml_file)
    pprint(data, indent=2)

    input_files = get_input_files(yaml_file, data)
    basename = data['ga_label']
    filename_format = 'combined_singlevar_{x}_{y}.nc'
    tile_options = {
        'output_format': 'GTiff',
        'create_options': ['COMPRESS=DEFLATE', 'ZLEVEL=1']
    }

    # Create Tiles
    create_tiles(input_files, output_dir, basename, tile_options)
    renames = calc_target_names('test.csv', filename_format, data)

    # Import into proper NetCDF files
    for geotiff, netcdf in renames:
        create_or_replace(geotiff, netcdf, data, netcdf_class=netcdf_class)


if __name__ == '__main__':
    try:
        from ipdb import launch_ipdb_on_exception
        with launch_ipdb_on_exception():
            main()
    except ImportError:
        main()
