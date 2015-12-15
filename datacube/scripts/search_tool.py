#!/usr/bin/env python
# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import
from __future__ import print_function

import csv
import datetime
import sys

import click
from dateutil import tz
from psycopg2._range import Range
from singledispatch import singledispatch

from datacube import config
from datacube.index import index_connect
from datacube.ui import parse_expressions

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
    write_csv(
        i.datasets.get_fields(),
        i.datasets.search_summaries(*parse_expressions(i.datasets.get_field, *expression)),
        sys.stdout
    )


@cli.command(help='Storage units')
@click.argument('expression',
                nargs=-1)
def units(expression):
    i = index_connect()
    write_csv(
        i.storage.get_fields(),
        i.storage.search_summaries(*parse_expressions(i.storage.get_field_with_fallback, *expression)),
        sys.stdout
    )


@singledispatch
def printable(val):
    return val


@printable.register(datetime.datetime)
def printable_dt(val):
    """
    :type val: datetime.datetime
    """
    # Default to UTC.
    if val.tzinfo is None:
        return val.replace(tzinfo=tz.tzutc()).isoformat()
    else:
        return val.astimezone(tz.tzutc()).isoformat()


@printable.register(Range)
def printable_r(val):
    """
    :type val: psycopg2._range.Range
    """
    if val.lower_inf:
        return val.upper
    if val.upper_inf:
        return val.lower

    return '{} to {}'.format(printable(val.lower), printable(val.upper))


def printable_values(d):
    return {k: printable(v) for k, v in d.items()}


def write_csv(fields, search_results, target_f):
    writer = csv.DictWriter(target_f, tuple(sorted(fields.keys())))
    writer.writeheader()
    writer.writerows(
        (
            printable_values(d) for d in
            search_results
        )
    )


if __name__ == '__main__':
    cli()
