"""
Create statistical summaries command

"""

import logging
import os
from datetime import datetime
from functools import partial
from itertools import groupby

import click
from dateutil import tz
from pathlib import Path

import datacube
from datacube.ui import task_app

_LOG = logging.getLogger(__name__)

APP_NAME = 'datacube-ncml'


def get_filename(config, cell_index, year=None):
    if year is None:
        file_path_template = str(Path(config['location'], config['ncml_path_template']))
    else:
        file_path_template = str(Path(config['location'], config['partial_ncml_path_template']))
    return file_path_template.format(tile_index=cell_index, start_time=year)


def make_ncml_tasks(index, config, cell_index=None, year=None, cell_index_list=None, **kwargs):
    product = config['product']

    query = {}
    if year is not None:
        query['time'] = datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1)

    gw = datacube.api.GridWorkflow(index=index, product=product.name)

    if cell_index_list is None:
        if cell_index is not None:
            cell_index_list = [cell_index]
        else:
            cell_index_list = []

    for requested_cell_index in cell_index_list:
        cells = gw.list_cells(product=product.name, cell_index=requested_cell_index, **query)
        for (found_cell_index, tile) in cells.items():
            output_filename = get_filename(config, found_cell_index, year)
            yield dict(tile=tile,
                       cell_index=found_cell_index,
                       output_filename=output_filename)


def make_ncml_config(index, config, export_path=None, nested_years=None, **query):
    config['product'] = index.products.get_by_name(config['output_type'])

    config['nested_years'] = nested_years if nested_years is not None else []

    if export_path is not None:
        config['location'] = export_path

    if not os.access(config['location'], os.W_OK):
        _LOG.warning('Current user appears not have write access output location: %s', config['location'])
    return config


def get_history_attribute(config, task):
    return '{dt} {user} {app} ({ver}) {args}  # {comment}'.format(
        dt=datetime.now(tz.tzlocal()).isoformat(),
        user=os.environ.get('USERNAME') or os.environ.get('USER'),
        app=APP_NAME,
        ver=datacube.__version__,
        args=', '.join([config['app_config_file'],
                        task['output_filename'],
                        str(task['cell_index'])]),
        comment='Created NCML file to aggregate multiple NetCDF files along the time dimension'
    )


def do_ncml_task(config, task):
    tile = task['tile']
    nested_years = config['nested_years']

    def get_sources_filepath(sources):
        year = int(str(sources.time.values.astype('datetime64[Y]')))
        if year in nested_years:
            file_path_template = str(Path(config['location'], config['partial_ncml_path_template']))
            return file_path_template.format(tile_index=task['cell_index'], start_time=year), True

        return str(sources.item()[0].local_path), False

    header_attrs = dict(date_created=datetime.today().isoformat(),
                        history=get_history_attribute(config, task))

    file_locations = []
    for (file_location, is_nested_ncml), sources in groupby(tile.sources, get_sources_filepath):
        file_locations.append(file_location)
        if is_nested_ncml:
            write_ncml_file(file_location, [str(source.item()[0].local_path) for source in sources], header_attrs)

    write_ncml_file(task['output_filename'], file_locations, header_attrs)


def write_ncml_file(ncml_filename, file_locations, header_attrs):
    filename = Path(ncml_filename)
    temp_filename = Path().joinpath(*filename.parts[:-1]) / '.tmp' / filename.parts[-1]

    if temp_filename.exists():
        temp_filename.unlink()

    try:
        temp_filename.parent.mkdir(parents=True)
    except OSError:
        pass

    netcdf_def = """
        <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="{path}">
            <remove name="dataset" type="variable" />
            <remove name="dataset_nchar" type="dimension" />
        </netcdf>"""

    with open(str(temp_filename), 'w') as ncml_file:
        ncml_file.write('<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">\n')

        for key, value in header_attrs.items():
            ncml_file.write("  <attribute name='{key}' type='string' value='{value}' />\n".format(key=key, value=value))

        ncml_file.write('  <aggregation dimName="time" type="joinExisting">\n')

        for file_location in file_locations:
            ncml_file.write(netcdf_def.format(path=file_location))

        ncml_file.write('  </aggregation>\n')
        ncml_file.write('</netcdf>\n')

    if filename.exists():
        filename.unlink()

    temp_filename.rename(filename)


@click.group(name=APP_NAME, help='NCML creation utility')
@datacube.ui.click.version_option
def ncml_app():
    pass


#: pylint: disable=invalid-name
command_options = datacube.ui.click.compose(
    datacube.ui.click.config_option,
    datacube.ui.click.pass_index(app_name=APP_NAME),
    datacube.ui.click.logfile_option,
    task_app.cell_index_option,
    task_app.cell_index_list_option,
    task_app.queue_size_option,
    task_app.load_tasks_option,
    task_app.save_tasks_option,
    datacube.ui.click.executor_cli_options,
    click.option('--export-path', 'export_path',
                 help='Write the stacked files to an external location instead of the location in the app config',
                 default=None,
                 type=click.Path(exists=True, writable=True, file_okay=False)),
)


@ncml_app.command(short_help='Create an ncml file')
@command_options
@click.argument('app_config')
@task_app.task_app(make_config=make_ncml_config, make_tasks=make_ncml_tasks)
def full(index, config, tasks, executor, queue_size, **kwargs):
    """Create ncml files for the full time depth of the product

    e.g. datacube-ncml full <app_config_yaml>
    """
    click.echo('Starting datacube ncml utility...')

    task_func = partial(do_ncml_task, config)
    task_app.run_tasks(tasks, executor, task_func, None, queue_size)


@ncml_app.command(short_help='Create a full ncml file with nested ncml files for particular years')
@command_options
@click.argument('app_config')
@click.argument('nested_years', nargs=-1, type=click.INT)
@task_app.task_app(make_config=make_ncml_config, make_tasks=make_ncml_tasks)
def nest(index, config, tasks, executor, queue_size, **kwargs):
    """Create ncml files for the full time, with nested ncml files covering the given years

    e.g. datacube-ncml nest <app_config_yaml> 2016 2017

    This will refer to the actual files (hopefully stacked), and make ncml files for the given (ie unstacked) years.
    Use the `update` command when new data is added to a year, without having to rerun for the entire time depth.
    """
    click.echo('Starting datacube ncml utility...')

    task_func = partial(do_ncml_task, config)
    task_app.run_tasks(tasks, executor, task_func, None, queue_size)


@ncml_app.command(short_help='Update a single year ncml file')
@command_options
@click.argument('app_config')
@click.argument('year', type=click.INT)
@task_app.task_app(make_config=make_ncml_config, make_tasks=make_ncml_tasks)
def update(index, config, tasks, executor, queue_size, **kwargs):
    """Update a single year ncml file

    e.g datacube-ncml <app_config_yaml> 1996

    This can be used to update an existing ncml file created with `nest` when new data is added.
    """
    click.echo('Starting datacube ncml utility...')

    task_func = partial(do_ncml_task, config)
    task_app.run_tasks(tasks, executor, task_func, None, queue_size)


if __name__ == '__main__':
    ncml_app()
