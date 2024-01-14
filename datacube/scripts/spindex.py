# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
import sys
from typing import Sequence

import click
import pyproj
from click import echo, confirm
from odc.geo import CRS

from datacube.index import Index
from datacube.ui import click as ui
from datacube.ui.click import cli

_LOG = logging.getLogger('datacube-system')


@cli.group(name='spindex', help='System commands')
def system():
    pass


@system.command(
    'create',
    help='Create unpopulated spatial index for particular CRSes')
@click.option(
    '--update/--no-update', '-u', is_flag=True, default=False,
    help="Populate the spatial index after creation (slow). "
         "For finer grained updating, use the 'spindex update' command"
)
@click.argument('srids', nargs=-1)
@ui.pass_index()
def create(index: Index, update: bool, srids: Sequence[str]):
    if not index.supports_spatial_indexes:
        echo("The active index driver does not support spatial indexes")
        exit(1)
    if not srids:
        echo("Must supply at least one CRS to create/update")
        exit(1)

    confirmed = []
    failed = []
    for srid in srids:
        try:
            crs = CRS(srid)
        except pyproj.exceptions.CRSError:
            failed.append(srid)
            echo(f"{srid} cannot be converted to a valid CRS")
            continue
        if not crs.epsg:
            failed.append(srid)
            echo(f"{srid} cannot be converted to a CRS with an EPSG number")
            continue
        if crs in index.spatial_indexes():
            # A spatial index for crs already exists: skip silently
            confirmed.append(crs)
        elif index.create_spatial_index(crs):
            # Creation succeeded
            confirmed.append(crs)
        else:
            # Creation attempted but failed
            failed.append(srid)
    if failed:
        str_failed = ','.join(f'{srid}' for srid in failed)
        echo(f"Could not create spatial indexes for: {str_failed}")
    if confirmed:
        echo(f"Spatial indexes created for: {','.join(str(crs.epsg) for crs in confirmed)}")
    if update and failed:
        echo("Skipping update")
    elif update:
        result = index.update_spatial_index(confirmed)
        echo(f'{result} extents checked and updated in spatial indexes')
    else:
        echo("Newly created spatial indexes are unpopulated - run 'datacube spindex update' before use.")
    exit(len(failed))


@system.command(
    "list",
    help="List all CRSs for which spatial indexes exist in this index"
)
@ui.pass_index()
def list_spindex(index):
    if not index.supports_spatial_indexes:
        echo("The active index driver does not support spatial indexes")
        exit(1)
    for crs in index.spatial_indexes():
        echo(f'EPSG:{crs.epsg}')
    exit(0)


@system.command(
    'update',
    help='Update a spatial index for particular CRSs.')
@click.option(
    '--product', '-p', multiple=True,
    help="The name of a product to update the spatial index for (can be used multiple times for multiple products)"
)
@click.option(
    '--dataset', '-d', multiple=True,
    help="The id of a dataset to update the spatial index for (can be used multiple times for multiple datasets)"
)
@click.argument('srids', nargs=-1)
@ui.pass_index()
def update(index: Index, product: Sequence[str], dataset: Sequence[str], srids: Sequence[str]):
    if not index.supports_spatial_indexes:
        echo("The active index driver does not support spatial indexes")
        exit(1)
    if not srids:
        echo("Must supply at least one CRS to update")
        exit(1)

    for_update = []
    cant_update = []
    for srid in srids:
        try:
            crs = CRS(srid)
        except pyproj.exceptions.CRSError:
            echo(f"{srid} is not a valid CRS: skipping")
            cant_update.append(srid)
            continue
        if not crs.epsg:
            echo(f"{srid} cannot be converted to a CRS with an EPSG number")
            cant_update.append(srid)
            continue

        if crs in index.spatial_indexes():
            for_update.append(crs)
        else:
            cant_update.append(srid)
            echo(f"No spatial index for crs {srid} exists: skipping")
    if not for_update:
        echo("Nothing to update!")
        exit(len(cant_update))
    result = index.update_spatial_index(for_update, product_names=product, dataset_ids=dataset)
    echo(f'{result} extents checked and updated in {len(for_update)} spatial indexes')
    exit(len(cant_update))


@system.command(
    'drop',
    help='Drop existing spatial indexes for particular CRSs')
@click.option(
    '--force/--no-force', '-f', is_flag=True, default=False,
    help="If set, does not ask the user to confirm deletion"
)
@click.argument('srids', nargs=-1)
@ui.pass_index()
def drop(index: Index, force: bool, srids: Sequence[str]):
    if not index.supports_spatial_indexes:
        echo("The active index driver does not support spatial indexes")
        exit(1)
    if not srids:
        echo("Must supply at least one CRS to drop")
        exit(1)
    for_deletion = []
    errors = False
    for srid in srids:
        try:
            crs = CRS(srid)
        except pyproj.exceptions.CRSError:
            echo(f"{srid} is not a valid CRS: skipping")
            errors = True
            continue
        if not crs.epsg:
            echo(f"{srid} cannot be converted to a CRS with an EPSG number")
            errors = True
            continue
        if crs in index.spatial_indexes():
            for_deletion.append(crs)
        else:
            echo(f"No spatial index exists for CRS {srid} - skipping")
    if for_deletion and not force:
        echo("WARNING: Recreating spatial indexes may be slow and expensive for large databases.")
        echo("You have requested to delete spatial indexes for the following "
             f"CRSes: {','.join(str(crs.epsg) for crs in for_deletion)}")
        if sys.stdin.isatty():
            confirmed = confirm(
                "Are you sure you want to delete these spatial indexes?",
                default=False)
            if not confirmed:
                echo('OK aborting', err=True)
                exit(1)
        else:
            echo("Use --force from non-interactive scripts. Aborting.")
            exit(1)
    for crs in for_deletion:
        click.echo(f"Deleting spatial index for CRS epsg:{crs.epsg}: ", nl=False)
        if index.drop_spatial_index(crs):
            click.echo("Done")
        else:
            click.echo("Failed")
            errors = True
    if errors:
        exit(1)
    else:
        exit(0)
