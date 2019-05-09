
import json
import logging
import sys
from typing import List

import yaml
from pathlib import Path

import click
from click import echo, style

from datacube.index.index import Index
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils import read_documents, InvalidDocException
from datacube.utils.serialise import SafeDatacubeDumper

_LOG = logging.getLogger('datacube-md-type')


@cli.group(name='metadata', help='Metadata type commands')
def this_group():
    pass


@this_group.command('add')
@click.option('--allow-exclusive-lock/--forbid-exclusive-lock', is_flag=True, default=False,
              help='Allow index to be locked from other users while updating (default: false)')
@click.argument('files',
                type=str,
                nargs=-1)
@ui.pass_index()
def add_metadata_types(index, allow_exclusive_lock, files):
    # type: (Index, bool, list) -> None
    """
    Add or update metadata types in the index
    """
    for descriptor_path, parsed_doc in read_documents(*files):
        try:
            type_ = index.metadata_types.from_doc(parsed_doc)
            index.metadata_types.add(type_, allow_table_lock=allow_exclusive_lock)
        except InvalidDocException as e:
            _LOG.exception(e)
            _LOG.error('Invalid metadata type definition: %s', descriptor_path)
            continue


@this_group.command('update')
@click.option(
    '--allow-unsafe/--forbid-unsafe', is_flag=True, default=False,
    help="Allow unsafe updates (default: false)"
)
@click.option('--allow-exclusive-lock/--forbid-exclusive-lock', is_flag=True, default=False,
              help='Allow index to be locked from other users while updating (default: false)')
@click.option('--dry-run', '-d', is_flag=True, default=False,
              help='Check if everything is ok')
@click.argument('files', type=str, nargs=-1)
@ui.pass_index()
def update_metadata_types(index: Index, allow_unsafe: bool, allow_exclusive_lock: bool, dry_run: bool, files: List):
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
            index.metadata_types.update(
                type_,
                allow_unsafe_updates=allow_unsafe,
                allow_table_lock=allow_exclusive_lock,
            )
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


@this_group.command('show')
@click.option('-f', 'output_format', help='Output format',
              type=click.Choice(['yaml', 'json']), default='yaml', show_default=True)
@click.argument('metadata_type_name', nargs=-1)
@ui.pass_index()
def show_metadata_type(index, metadata_type_name, output_format):
    """
    Show information about a metadata type.
    """

    if len(metadata_type_name) == 0:
        mm = list(index.metadata_types.get_all())
    else:
        mm = []
        for name in metadata_type_name:
            m = index.metadata_types.get_by_name(name)
            if m is None:
                echo('No such metadata: {!r}'.format(name), err=True)
                sys.exit(1)
            else:
                mm.append(m)

    if len(mm) == 0:
        echo('No metadata')
        sys.exit(1)

    if output_format == 'yaml':
        yaml.dump_all((m.definition for m in mm),
                      sys.stdout,
                      Dumper=SafeDatacubeDumper,
                      default_flow_style=False,
                      indent=4)
    elif output_format == 'json':
        if len(mm) > 1:
            echo('Can not output more than 1 metadata document in json format', err=True)
            sys.exit(1)
        m = mm[0]
        echo(json.dumps(m.definition, indent=4))


@this_group.command('list')
@ui.pass_index()
def list_metadata_types(index):
    """
    List metadata types that are defined in the generic index.
    """
    metadata_types = list(index.metadata_types.get_all())

    if not metadata_types:
        echo('No metadata types found :(', err=True)
        sys.exit(1)

    max_w = max(len(m.name) for m in metadata_types)
    for m in metadata_types:
        description_short = m.definition.get('description', '').split('\n')[0]
        name = '{s:<{n}}'.format(s=m.name, n=max_w)
        echo(style(name, fg='green') + '  ' + description_short)
