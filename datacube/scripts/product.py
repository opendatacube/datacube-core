from __future__ import absolute_import

import json
import logging
from pathlib import Path

import click
from click import echo

from datacube import Datacube
from datacube.index._api import Index
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils import read_documents, InvalidDocException

_LOG = logging.getLogger('datacube-product')


@cli.group(name='product', help='Product commands')
def product():
    pass


@product.command('add')
@click.option('--allow-exclusive-lock/--forbid-exclusive-lock', is_flag=True, default=False,
              help='Allow index to be locked from other users while updating (default: false)')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def add_dataset_types(index, allow_exclusive_lock, files):
    # type: (Index, bool, list) -> None
    """
    Add or update products in the index
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


@product.command('update')
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
def update_dataset_types(index, allow_unsafe, allow_exclusive_lock, dry_run, files):
    # type: (Index, bool, bool, bool, list) -> None
    """
    Update existing products.

    An error will be thrown if a change is potentially unsafe.

    (An unsafe change is anything that may potentially make the product
    incompatible with existing datasets of that type)
    """
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.products.from_doc(parsed_doc)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid product definition: %s', descriptor_path)
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
        else:
            can_update, safe_changes, unsafe_changes = index.products.can_update(type_,
                                                                                 allow_unsafe_updates=allow_unsafe)

            for offset, old_val, new_val in safe_changes:
                echo('Safe change in "%s" from %r to %r' % (type_.name, old_val, new_val))

            for offset, old_val, new_val in unsafe_changes:
                echo('Unsafe change in "%s" from %r to %r' % (type_.name, old_val, new_val))

            if can_update:
                echo('Can update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                              len(unsafe_changes),
                                                                              len(safe_changes)))
            else:
                echo('Cannot update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                                 len(unsafe_changes),
                                                                                 len(safe_changes)))


@product.command('list')
@ui.pass_index()
def list_products(index):
    """
    List products that are defined in the index
    """
    dc = Datacube(index)
    products = dc.list_products()

    if products.empty:
        echo('No products discovered :(')
        return

    echo(products.to_string(columns=('name', 'description', 'product_type', 'instrument',
                                     'format', 'platform'),
                            justify='left'))


@product.command('show')
@click.argument('product_name', nargs=1)
@ui.pass_index()
def show_product(index, product_name):
    """
    Show details about a product in the index
    """
    product_def = index.products.get_by_name(product_name)
    click.echo_via_pager(json.dumps(product_def.definition, indent=4))
