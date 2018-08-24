from __future__ import absolute_import

import json
import logging
from pathlib import Path

import click
import sys
from click import echo

import yaml
import yaml.resolver
import csv
from datacube.api.core import dataset_type_to_row
import pandas as pd

from datacube.index.index import Index
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils import read_documents, InvalidDocException
from datacube.utils.serialise import SafeDatacubeDumper

_LOG = logging.getLogger('datacube-product')


@cli.group(name='product', help='Product commands')
def product_cli():
    pass


@product_cli.command('add')
@click.option('--allow-exclusive-lock/--forbid-exclusive-lock', is_flag=True, default=False,
              help='Allow index to be locked from other users while updating (default: false)')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def add_dataset_types(index, allow_exclusive_lock, files):
    # type: (Index, bool, list) -> None
    """
    Add or update products in the generic index.
    """
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.products.from_doc(parsed_doc)
            index.products.add(type_, allow_table_lock=allow_exclusive_lock)
            echo('Added "%s"' % type_.name)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid product definition: %s', descriptor_path)
            continue


@product_cli.command('update')
@click.option(
    '--allow-unsafe/--forbid-unsafe', is_flag=True, default=False,
    help="Allow unsafe updates (default: false)"
)
@click.option('--allow-exclusive-lock/--forbid-exclusive-lock', is_flag=True, default=False,
              help='Allow index to be locked from other users while updating (default: false)')
@click.option('--dry-run', '-d', is_flag=True, default=False,
              help='Check if everything is ok')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def update_products(index, allow_unsafe, allow_exclusive_lock, dry_run, files):
    # type: (Index, bool, bool, bool, list) -> None
    """
    Update existing products.

    An error will be thrown if a change is potentially unsafe.

    (An unsafe change is anything that may potentially make the product
    incompatible with existing datasets of that type)
    """
    failures = 0
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.products.from_doc(parsed_doc)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid product definition: %s', descriptor_path)
            failures += 1
            continue

        if not dry_run:
            try:
                index.products.update(
                    type_,
                    allow_unsafe_updates=allow_unsafe,
                    allow_table_lock=allow_exclusive_lock,
                )
                echo('Updated "%s"' % type_.name)
            except ValueError as e:
                echo('Failed to update "%s": %s' % (type_.name, e))
                failures += 1
        else:
            can_update, safe_changes, unsafe_changes = index.products.can_update(
                type_,
                allow_unsafe_updates=allow_unsafe
            )

            if can_update:
                echo('Can update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                              len(unsafe_changes),
                                                                              len(safe_changes)))
            else:
                echo('Cannot update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                                 len(unsafe_changes),
                                                                                 len(safe_changes)))
    sys.exit(failures)


def build_product_list(index):
    lstdct = []
    for product in index.products.search():
        info = dataset_type_to_row(product)
        lstdct.append(info)
    return lstdct


def _write_csv(index):
    writer = csv.DictWriter(sys.stdout, ['id', 'name', 'description',
                                         'ancillary_quality', 'latgqa_cep90', 'product_type',
                                         'gqa_abs_iterative_mean_xy', 'gqa_ref_source', 'sat_path',
                                         'gqa_iterative_stddev_xy', 'time', 'sat_row', 'orbit', 'gqa',
                                         'instrument', 'gqa_abs_xy', 'crs', 'resolution', 'tile_size',
                                         'spatial_dimensions'], extrasaction='ignore')
    writer.writeheader()

    def add_first_name(row):
        names_ = row['name']
        row['name'] = names_ if names_ else None
        return row

    writer.writerows(add_first_name(row) for row in index)


def _write_yaml(index):
    """
    Dump yaml data with support for OrderedDicts.

    Allows for better human-readability of output: such as dataset ID field first, sources last.

    (Ordered dicts are output identically to normal yaml dicts: their order is purely for readability)
    """

    try:
        return yaml.dump_all(index, sys.stdout, Dumper=SafeDatacubeDumper, default_flow_style=False, indent=4)
    except TypeError:
        return yaml.dump(index.definition, sys.stdout, Dumper=SafeDatacubeDumper, default_flow_style=False, indent=4)


def _write_tab(products):
    products = pd.DataFrame(products)

    if products.empty:
        echo('No products discovered :(')
        return

    echo(products.to_string(columns=('id', 'name', 'description', 'ancillary_quality',
                                     'product_type', 'gqa_abs_iterative_mean_xy',
                                     'gqa_ref_source', 'sat_path',
                                     'gqa_iterative_stddev_xy', 'time', 'sat_row',
                                     'orbit', 'gqa', 'instrument', 'gqa_abs_xy', 'crs',
                                     'resolution', 'tile_size', 'spatial_dimensions'),
                            justify='left'))


LIST_OUTPUT_WRITERS = {
    'csv': _write_csv,
    'yaml': _write_yaml,
    'tab': _write_tab,
}


@product_cli.command('list')
@click.option('-f', help='Output format',
              type=click.Choice(list(LIST_OUTPUT_WRITERS)), default='yaml', show_default=True)
@ui.pass_datacube()
def list_products(dc, f):
    """
    List products that are defined in the generic index.
    """
    LIST_OUTPUT_WRITERS[f](build_product_list(dc.index))


def build_product_show(index, product_name):
    product_def = index.products.get_by_name(product_name)
    return product_def


def _write_json(product_def):
    click.echo_via_pager(json.dumps(product_def.definition, indent=4))


SHOW_OUTPUT_WRITERS = {
    'yaml': _write_yaml,
    'json': _write_json,
}


@product_cli.command('show')
@click.option('-f', help='Output format',
              type=click.Choice(list(SHOW_OUTPUT_WRITERS)), default='yaml', show_default=True)
@click.argument('product_name', nargs=1)
@ui.pass_datacube()
def show_product(dc, product_name, f):
    """
    Show details about a product in the generic index.
    """
    SHOW_OUTPUT_WRITERS[f](build_product_show(dc.index, product_name))
