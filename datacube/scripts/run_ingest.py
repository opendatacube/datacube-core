# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import logging

import click
from pathlib import Path

from datacube.ingest import ingest

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(help="Ingest datasets into the Data Cube.", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.argument('dataset',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def cli(verbose, dataset):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.WARN)
    logging.getLogger('datacube').setLevel(logging.WARN - 10 * verbose)

    for dataset_path in dataset:
        ingest(Path(dataset_path))

if __name__ == '__main__':
    cli()
