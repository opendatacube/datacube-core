from __future__ import absolute_import

import logging
import click
from click import echo

from pathlib import Path

from datacube import Datacube
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils import read_documents, InvalidDocException

_LOG = logging.getLogger('datacube-product')


@cli.group(name='product', help='Product commands')
def product():
    pass


@product.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def add_dataset_types(index, files):
    """
    Add product types to the index
    """
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.products.from_doc(parsed_doc)
            index.products.add(type_)
            echo('Added "%s"' % type_.name)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid product definition: %s', descriptor_path)
            continue


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
