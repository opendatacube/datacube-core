"""
Create statistical summaries command

"""
from __future__ import absolute_import, print_function

import logging
import os
from datetime import datetime
from functools import partial

import click
from dateutil import tz
from pathlib import Path

import datacube
from datacube.ui import task_app
from datacube.ui.click import to_pathlib


_LOG = logging.getLogger(__name__)


APP_NAME = 'datacube-ncml'


def get_filename(config, cell_index):
    file_path_template = str(Path(config['location'], config['ncml_path_template']))
    return file_path_template.format(tile_index=cell_index)


def make_ncml_tasks(index, config, cell_index=None, **kwargs):
    product = config['product']

    gw = datacube.api.GridWorkflow(index=index, product=product.name)
    cells = gw.list_cells(product=product.name, cell_index=cell_index)
    for (cell_index, tile) in cells.items():
        output_filename = get_filename(config, cell_index)
        yield dict(tile=tile,
                   cell_index=cell_index,
                   output_filename=output_filename)


def make_ncml_config(index, config, export_path=None, **query):
    config['product'] = index.products.get_by_name(config['output_type'])

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
                        str(config['version']),
                        task['output_filename'],
                        str(task['cell_index'])
                       ]),
        comment='Created NCML file to aggregate multiple NetCDF files along the time dimension'
    )


def do_ncml_task(config, task):
    tile = task['tile']
    ncml_filename = task['output_filename']

    ncml_header = """<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
      <attribute name='date_created' type='string' value='{date_created}' />
      <attribute name='history' type='string' value='{history}' />
      <aggregation dimName="time" type="joinExisting">"""

    ncml_footer = """
      </aggregation>
    </netcdf>"""

    netcdf_def = """
        <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="{path}">
            <remove name="dataset" type="variable" />
            <remove name="dataset_nchar" type="dimension" />
        </netcdf>"""

    with open(ncml_filename, 'w') as ncml_file:
        ncml_file.write(ncml_header.format(date_created=datetime.today().isoformat(),
                                           history=get_history_attribute(config, task)))
        for timeslice_sources in tile.sources.values:
            ncml_file.write(netcdf_def.format(path=str(timeslice_sources[0].local_path)))
        ncml_file.write(ncml_footer)


@click.command(name=APP_NAME)
@datacube.ui.click.pass_index(app_name=APP_NAME)
@datacube.ui.click.global_cli_options
@click.option('--cell-index', 'cell_index', help='Limit the process to a particular cell (e.g. 14,-11)',
              callback=task_app.validate_cell_index, default=None)
@task_app.queue_size_option
@task_app.task_app_options
@task_app.task_app(make_config=make_ncml_config, make_tasks=make_ncml_tasks)
def main(index, config, tasks, executor, queue_size, **kwargs):
    click.echo('Starting datacube ncml utility...')

    task_func = partial(do_ncml_task, config)
    task_app.run_tasks(tasks, executor, task_func, None, queue_size)


if __name__ == '__main__':
    main()
