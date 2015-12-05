# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

from pathlib import Path

import click
from datacube import config, index
from datacube.ingest import index_datasets, store_datasets


CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(help="Ingest datasets into the Data Cube.", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.option('--log-queries', is_flag=True, help="Print database queries.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def cli(verbose, datasets, log_queries):
    config.init_logging(verbosity_level=verbose, log_queries=log_queries)

    datasets = []
    i = index.index_connect()
    for dataset_path in datasets:
        datasets += index_datasets(Path(dataset_path), index=i)

    store_datasets(datasets, index=i)


if __name__ == '__main__':
    cli()
