from __future__ import absolute_import, print_function

import logging
from pathlib import Path
from pprint import pprint

import click
from click import echo
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils import read_documents, InvalidDocException

_LOG = logging.getLogger('datacube-md-type')


@cli.group(name='metadata_type', help='Metadata type commands')
def metadata_type():
    pass


@metadata_type.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def add_metadata_types(index, files):
    """
    Add or update metadata types in the index
    """
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.metadata_types.from_doc(parsed_doc)
            index.metadata_types.add(type_)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid metadata type definition: %s', descriptor_path)
            continue


@metadata_type.command('update')
@click.option(
    '--allow-unsafe/--forbid-unsafe', is_flag=True, default=False,
    help="Allow unsafe updates (default: false)"
)
@click.option('--dry-run', '-d', is_flag=True, default=False,
              help='Check if everything is ok')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index()
def update_metadata_types(index, allow_unsafe, dry_run, files):
    """
    Update existing metadata types.

    An error will be thrown if a change is potentially unsafe.

    (An unsafe change is anything that may potentially make the metadata type
    incompatible with existing ones of the same name)
    """
    for descriptor_path, parsed_doc in read_documents(*(Path(f) for f in files)):
        try:
            type_ = index.metadata_types.from_doc(parsed_doc)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid metadata type definition: %s', descriptor_path)
            continue

        if not dry_run:
            index.metadata_types.update(type_, allow_unsafe_updates=allow_unsafe)
            echo('Updated "%s"' % type_.name)
        else:
            can_update, safe_changes, unsafe_changes = index.metadata_types.can_update(
                type_, allow_unsafe_updates=allow_unsafe
            )
            if can_update:
                echo('Can update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                              len(unsafe_changes),
                                                                              len(safe_changes)))
            else:
                echo('Cannot update "%s": %s unsafe changes, %s safe changes' % (type_.name,
                                                                                 len(unsafe_changes),
                                                                                 len(safe_changes)))


@metadata_type.command('show')
@click.option('-v', '--verbose', is_flag=True)
@click.argument('metadata_type_name', nargs=1)
@ui.pass_index()
def show_metadata_type(index, metadata_type_name, verbose):
    """
    Show information about a metadata type.
    """
    metadata_type_obj = index.metadata_types.get_by_name(metadata_type_name)
    print(metadata_type_obj.description)
    print('Search fields: %s' % ', '.join(sorted(metadata_type_obj.dataset_fields.keys())))
    if verbose:
        pprint(metadata_type_obj.definition, width=100)


@metadata_type.command('list')
@ui.pass_index()
def list_metadata_types(index):
    """
    List metadata types that are defined in the index
    """
    metadata_types = list(index.metadata_types.get_all())

    if not metadata_types:
        echo('No metadata types found :(')
        return

    for m in metadata_types:
        echo(m)
