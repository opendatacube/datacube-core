# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import
from __future__ import print_function

import click

from datacube.ui import parse_expressions
from datacube import config
from datacube.index import index_connect


CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(help="Search the Data Cube", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.option('--log-queries', is_flag=True, help="Print database queries.")
def cli(verbose, log_queries):
    config.init_logging(verbosity_level=verbose, log_queries=log_queries)


@cli.command(help='Datasets')
@click.argument('expression',
                nargs=-1)
def datasets(expression):
    i = index_connect()

    for d in i.datasets.search(*parse_expressions(i.datasets.get_field, *expression)):
        print(str(d))


@cli.command(help='Storage units')
@click.argument('expression',
                nargs=-1)
def units(expression):
    i = index_connect()

    for su in i.storage.search(*parse_expressions(i.storage.get_field_with_fallback, *expression)):
        print(str(su))


if __name__ == '__main__':
    cli()
