# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import csv
import datetime
import logging
import sys
from collections import OrderedDict
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from textwrap import dedent
from typing import Any, cast
from uuid import UUID

import click
import yaml
import yaml.resolver
from click import echo

from datacube.index import Index
from datacube.index.eo3 import prep_eo3
from datacube.index.exceptions import MissingRecordError
from datacube.index.hl import Doc2Dataset, check_dataset_consistent
from datacube.model import Dataset
from datacube.ui import click as ui
from datacube.ui.click import cli, print_help_msg
from datacube.ui.common import ui_path_doc_stream
from datacube.utils import SimpleDocNav, changes
from datacube.utils.serialise import SafeDatacubeDumper
from datacube.utils.uris import uri_resolve

_LOG: logging.Logger = logging.getLogger("datacube-dataset")


def report_old_options(mapping):
    def maybe_remap(s):
        if s in mapping:
            _LOG.warning(
                "DEPRECATED option detected: --%s use --%s instead", s, mapping[s]
            )
            return mapping[s]
        else:
            return s

    return maybe_remap


def _resolve_uri(uri, doc):
    loc = doc.location
    if loc is None:
        return uri

    if isinstance(loc, str):
        return loc

    if isinstance(loc, list | tuple):
        if len(loc) > 0:
            return loc[0]

    return uri


def remap_uri_from_doc(doc_stream) -> Iterator:
    """
    Given a stream of `uri: str, doc: dict` tuples, replace `uri` with `doc.location` if it is set.
    """
    for uri, doc in doc_stream:
        real_uri = _resolve_uri(uri, doc)
        yield real_uri, doc.without_location()


@cli.group(name="dataset", help="Dataset management commands")
def dataset_cmd() -> None:
    pass


def dataset_stream(doc_stream, ds_resolve) -> Iterator:
    """Convert a stream `(uri, doc)` pairs into a stream of resolved datasets

    skips failures with logging
    """
    for uri, ds in doc_stream:
        dataset, err = ds_resolve(ds, uri)

        if dataset is None:
            _LOG.error("%s", str(err))
            continue

        yield dataset


def load_datasets_for_update(doc_stream, index) -> Iterator:
    """Consume stream of dataset documents, associate each to a product by looking
    up existing dataset in the index. Datasets not in the database will be
    logged.

    Doesn't load lineage information

    Generates tuples in the form (new_dataset, existing_dataset)
    """

    def mk_dataset(ds, uri):
        uuid = ds.id

        if uuid is None:
            return None, None, "Metadata document it missing id field"

        existing = index.datasets.get(uuid)
        if existing is None:
            return None, None, f"No such dataset in the database: {uuid}"

        ds = SimpleDocNav(
            prep_eo3(ds.doc, auto_skip=True),
            sources_path=("lineage",)
            if index.supports_external_lineage
            else ("lineage", "source_datasets"),
        )

        # TODO: what about lineage?
        return (
            Dataset(existing.product, ds.doc_without_lineage_sources, uri=uri),
            existing,
            None,
        )

    for uri, doc in doc_stream:
        dataset, existing, error_msg = mk_dataset(doc, uri)

        if dataset is None:
            _LOG.error("Failure while processing: %s\n > Reason: %s", uri, error_msg)
        else:
            is_consistent, reason = check_dataset_consistent(dataset)
            if is_consistent:
                yield dataset, existing
            else:
                _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)


@dataset_cmd.command(
    "add",
    help="Add datasets to the Data Cube",
    context_settings={
        "token_normalize_func": report_old_options({"dtype": "product", "t": "p"})
    },
)
@click.option(
    "--product",
    "-p",
    "product_names",
    help=(
        "Only match against products specified with this option, "
        "you can supply several by repeating this option with a new product name"
    ),
    multiple=True,
)
@click.option(
    "--exclude-product",
    "-x",
    "exclude_product_names",
    help=(
        "Attempt to match to all products in the DB except for products "
        "specified with this option, "
        "you can supply several by repeating this option with a new product name"
    ),
    multiple=True,
)
@click.option(
    "--auto-add-lineage/--no-auto-add-lineage",
    is_flag=True,
    default=True,
    help=(
        "WARNING: will be deprecated in datacube v1.9.\n"
        "Default behaviour is to automatically add lineage datasets if they are missing from the database, "
        "but this can be disabled if lineage is expected to be present in the DB, "
        "in this case add will abort when encountering missing lineage dataset"
    ),
)
@click.option(
    "--verify-lineage/--no-verify-lineage",
    is_flag=True,
    default=True,
    help=(
        "WARNING: will be deprecated in datacube v1.9.\n"
        "Lineage referenced in the metadata document should be the same as in DB, "
        "default behaviour is to skip those top-level datasets that have lineage data "
        "different from the version in the DB. This option allows omitting verification step."
    ),
)
@click.option(
    "--dry-run", help="Check if everything is ok", is_flag=True, default=False
)
@click.option(
    "--ignore-lineage",
    help="Pretend that there is no lineage data in the datasets being indexed",
    is_flag=True,
    default=False,
)
@click.option(
    "--confirm-ignore-lineage",
    help=(
        "WARNING: this flag has been deprecated and will be removed in datacube v1.9.\n"
        "Pretend that there is no lineage data in the datasets being indexed, without confirmation"
    ),
    is_flag=True,
    default=False,
)
@click.option(
    "--archive-less-mature",
    help="Archive less mature versions of the dataset",
    is_flag=True,
    default=False,
)
@click.argument("dataset-paths", type=str, nargs=-1)
@ui.pass_index()
def index_cmd(
    index: Index,
    product_names: Sequence[str],
    exclude_product_names: Sequence[str],
    auto_add_lineage,
    verify_lineage,
    dry_run,
    ignore_lineage,
    confirm_ignore_lineage,
    archive_less_mature,
    dataset_paths: list[str],
) -> None:
    if not dataset_paths:
        click.echo("Error: no datasets provided\n")
        print_help_msg(index_cmd)
        sys.exit(1)

    confirm_ignore_lineage = ignore_lineage

    try:
        ds_resolve = Doc2Dataset(
            index,
            product_names,
            exclude_products=exclude_product_names,
            skip_lineage=confirm_ignore_lineage,
            fail_on_missing_lineage=not auto_add_lineage,
            verify_lineage=verify_lineage,
        )
    except ValueError as e:
        _LOG.error(e)
        sys.exit(2)

    def run_it(dataset_paths) -> None:
        doc_stream = ui_path_doc_stream(dataset_paths, logger=_LOG, uri=True)
        doc_stream = remap_uri_from_doc(doc_stream)
        dss = dataset_stream(doc_stream, ds_resolve)
        index_datasets(
            dss,
            index,
            auto_add_lineage=auto_add_lineage and not confirm_ignore_lineage,
            dry_run=dry_run,
            archive_less_mature=archive_less_mature,
        )

    # If outputting directly to terminal, show a progress bar.
    if sys.stdout.isatty():
        with click.progressbar(dataset_paths, label="Indexing datasets") as pp:
            run_it(pp)
    else:
        run_it(dataset_paths)


def index_datasets(dss, index, auto_add_lineage, dry_run, archive_less_mature) -> None:
    for dataset in dss:
        _LOG.info("Matched %s", dataset)
        if not dry_run:
            try:
                index.datasets.add(
                    dataset,
                    with_lineage=auto_add_lineage,
                    archive_less_mature=archive_less_mature,
                )
            except (ValueError, MissingRecordError) as e:
                _LOG.error("Failed to add dataset %s: %s", dataset.local_uri, e)


def parse_update_rules(keys_that_can_change) -> dict:
    updates_allowed = {}
    for key_str in keys_that_can_change:
        updates_allowed[tuple(key_str.split("."))] = changes.allow_any
    return updates_allowed


@dataset_cmd.command("update", help="Update datasets in the Data Cube")
@click.option(
    "--allow-any",
    "keys_that_can_change",
    help="Allow any changes to the specified key (a.b.c)",
    multiple=True,
)
@click.option(
    "--dry-run", help="Check if everything is ok", is_flag=True, default=False
)
@click.option(
    "--location-policy",
    type=click.Choice(["keep", "archive", "forget"]),
    default="keep",
    help=dedent("""
              What to do with previously recorded dataset location(s)

              \b
              - 'keep': keep as alternative location [default]
              - 'archive': mark as archived
              - 'forget': remove from the index"""),
)
@click.option(
    "--archive-less-mature",
    help="Archive less mature versions of the dataset",
    is_flag=True,
    default=False,
)
@click.argument("dataset-paths", nargs=-1)
@ui.pass_index()
def update_cmd(
    index,
    keys_that_can_change,
    dry_run,
    location_policy,
    dataset_paths,
    archive_less_mature,
) -> None:
    if not dataset_paths:
        click.echo("Error: no datasets provided\n")
        print_help_msg(update_cmd)
        sys.exit(1)

    def loc_action(action, new_ds, existing_ds, action_name: str) -> bool | None:
        if existing_ds.uri is None:
            return None

        if existing_ds.has_multiple_uris():
            _LOG.warning("Refusing to %s old location, there are several", action_name)
            return None

        if new_ds.has_multiple_uris():
            raise ValueError("More than one uri in new dataset")

        new_uri = new_ds.uri
        old_uri = existing_ds.uri

        if new_uri == old_uri:
            return None

        if dry_run:
            echo(
                f"Will {action_name} old location {old_uri}, and add new one {new_uri}"
            )
            return True

        return action(existing_ds.id, old_uri)

    def loc_archive(new_ds, existing_ds):
        return loc_action(
            index.datasets.archive_location, new_ds, existing_ds, "archive"
        )

    def loc_forget(new_ds, existing_ds):
        return loc_action(index.datasets.remove_location, new_ds, existing_ds, "forget")

    def loc_keep(new_ds, existing_ds):
        return None

    update_loc = {"archive": loc_archive, "forget": loc_forget, "keep": loc_keep}[
        location_policy
    ]

    updates_allowed = parse_update_rules(keys_that_can_change)

    success, fail = 0, 0
    doc_stream = ui_path_doc_stream(dataset_paths, logger=_LOG, uri=True)
    doc_stream = remap_uri_from_doc(doc_stream)

    for dataset, existing_ds in load_datasets_for_update(doc_stream, index):
        _LOG.info("Matched %s", dataset)

        if location_policy != "keep":
            if existing_ds.has_multiple_uris():
                # TODO:
                pass

        if not dry_run:
            try:
                index.datasets.update(
                    dataset,
                    updates_allowed=updates_allowed,
                    archive_less_mature=archive_less_mature,
                )
                update_loc(dataset, existing_ds)
                success += 1
                echo(f"Updated {dataset.id}")
            except ValueError as e:
                fail += 1
                echo(f"Failed to update {dataset.id}: {e}")
        else:
            if update_dry_run(index, updates_allowed, dataset):
                update_loc(dataset, existing_ds)
                success += 1
            else:
                fail += 1
    echo(f"{success} successful, {fail} failed")


def update_dry_run(index, updates_allowed, dataset) -> bool:
    try:
        can_update, safe_changes, unsafe_changes = index.datasets.can_update(
            dataset, updates_allowed=updates_allowed
        )
    except ValueError as e:
        echo(f"Cannot update {dataset.id}: {e}")
        return False

    if can_update:
        echo(
            f"Can update {dataset.id}: {len(unsafe_changes)} unsafe changes, {len(safe_changes)} safe changes"
        )
    else:
        echo(
            f"Cannot update {dataset.id}: {len(unsafe_changes)} unsafe changes, {len(safe_changes)} safe changes"
        )
    return can_update


def build_dataset_info(
    index: Index,
    dataset: Dataset,
    show_sources: bool = False,
    show_derived: bool = False,
    depth: int = 1,
    max_depth: int = 99,
) -> Mapping[str, Any]:
    info: MutableMapping[str, Any] = OrderedDict(
        (
            ("id", str(dataset.id)),
            ("product", dataset.product.name),
            ("status", "archived" if dataset.is_archived else "active"),
        )
    )

    # Optional when loading a dataset.
    if dataset.indexed_time is not None:
        info["indexed"] = dataset.indexed_time

    info["location"] = dataset.uri
    info["fields"] = dataset.metadata.fields

    if depth < max_depth:
        if show_sources and dataset.sources is not None:
            info["sources"] = {
                key: build_dataset_info(
                    index,
                    source,
                    show_sources=True,
                    show_derived=False,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
                for key, source in dataset.sources.items()
            }

        if show_derived:
            info["derived"] = [
                build_dataset_info(
                    index,
                    derived,
                    show_sources=False,
                    show_derived=True,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
                for derived in index.datasets.get_derived(dataset.id)
            ]

    return info


def _write_csv(infos) -> None:
    writer = csv.DictWriter(
        sys.stdout, ["id", "status", "product", "location"], extrasaction="ignore"
    )
    writer.writeheader()

    writer.writerows(row for row in infos)


def _write_yaml(infos):
    """
    Dump yaml data with support for OrderedDicts.

    Allows for better human-readability of output: such as dataset ID field first, sources last.

    (Ordered dicts are output identically to normal yaml dicts: their order is purely for readability)
    """
    return yaml.dump_all(
        infos, sys.stdout, SafeDatacubeDumper, default_flow_style=False, indent=4
    )


_OUTPUT_WRITERS = {
    "csv": _write_csv,
    "yaml": _write_yaml,
}


@dataset_cmd.command("info", help="Display dataset information")
@click.option(
    "--show-sources", help="Also show source datasets", is_flag=True, default=False
)
@click.option(
    "--show-derived", help="Also show derived datasets", is_flag=True, default=False
)
@click.option(
    "-f",
    help="Output format",
    type=click.Choice(list(_OUTPUT_WRITERS)),
    default="yaml",
    show_default=True,
)
@click.option(
    "--max-depth",
    help="Maximum sources/derived depth to travel",
    type=int,
    # Unlikely to be hit, but will avoid total-death by circular-references.
    default=99,
)
@click.argument("ids", nargs=-1)
@ui.pass_index()
def info_cmd(
    index: Index,
    show_sources: bool,
    show_derived: bool,
    f: str,
    max_depth: int,
    ids: Iterable[str],
) -> None:
    if not ids:
        click.echo("Error: no datasets provided\n")
        print_help_msg(info_cmd)
        sys.exit(1)

    # Using an array wrapper to get around the lack of "nonlocal" in py2
    missing_datasets = [0]

    def get_datasets(ids):
        for id_ in ids:
            dataset = index.datasets.get(id_, include_sources=show_sources)
            if dataset:
                yield dataset
            else:
                click.echo(f"{id_} missing", err=True)
                missing_datasets[0] += 1

    _OUTPUT_WRITERS[f](
        build_dataset_info(
            index,
            dataset,
            show_sources=show_sources,
            show_derived=show_derived,
            max_depth=max_depth,
        )
        for dataset in get_datasets(ids)
    )
    sys.exit(missing_datasets[0])


@dataset_cmd.command("search")
@click.option("--limit", help="Limit the number of results", type=int, default=None)
@click.option(
    "-f",
    help="Output format",
    type=click.Choice(list(_OUTPUT_WRITERS)),
    default="yaml",
    show_default=True,
)
@ui.parsed_search_expressions
@ui.pass_index()
def search_cmd(index: Index, limit: int, f: str, expressions) -> None:
    """
    Search available Datasets
    """
    datasets = index.datasets.search(limit=limit, **expressions)
    _OUTPUT_WRITERS[f](build_dataset_info(index, dataset) for dataset in datasets)


def _get_derived_set(index: Index, id_: UUID) -> set[Dataset]:
    """
    Get a single flat set of all derived datasets.
    (children, grandchildren, great-grandchildren...)
    """
    derived_set = {cast(Dataset, index.datasets.get(id_))}
    to_process = {id_}
    while to_process:
        derived = index.datasets.get_derived(to_process.pop())
        to_process.update(d.id for d in derived)
        derived_set.update(derived)
    return derived_set


@dataset_cmd.command("uri-search")
@click.option(
    "--search-mode",
    help="Exact, prefix or guess based searching",
    type=click.Choice(["exact", "prefix", "guess"]),
    default="prefix",
)
@click.argument("paths", nargs=-1)
@ui.pass_index()
def uri_search_cmd(index: Index, paths: list[str], search_mode) -> None:
    """
    Search by dataset locations

    PATHS may be either file paths or URIs
    """
    if not paths:
        click.echo("Error: no locations provided\n")
        print_help_msg(uri_search_cmd)
        sys.exit(1)

    if search_mode == "guess":
        # This is what the API expects. I think it should be changed.
        search_mode = None
    for path in paths:
        datasets = list(
            index.datasets.get_datasets_for_location(
                uri_resolve(base=path), mode=search_mode
            )
        )
        if not datasets:
            _LOG.info(f"Not found in index: {path}")
        for dataset in datasets:
            print(dataset)


@dataset_cmd.command("archive", help="Archive datasets")
@click.option(
    "--archive-derived",
    "-d",
    help="Also recursively archive derived datasets",
    is_flag=True,
    default=False,
)
@click.option(
    "--dry-run",
    help="Don't archive. Display datasets that would get archived",
    is_flag=True,
    default=False,
)
@click.option(
    "--all",
    "all_ds",
    help="Ignore id list - archive ALL non-archived datasets  (warning: may be slow on large databases)",
    is_flag=True,
    default=False,
)
@click.argument("ids", nargs=-1)
@ui.pass_index()
def archive_cmd(
    index: Index, archive_derived: bool, dry_run: bool, all_ds: bool, ids: list[str]
) -> None:
    if not ids and not all_ds:
        click.echo("Error: no datasets provided\n")
        print_help_msg(archive_cmd)
        sys.exit(1)

    derived_dataset_ids: list[UUID] = []
    if all_ds:
        datasets_for_archive = dict.fromkeys(
            index.datasets.get_all_dataset_ids(archived=False), True
        )
    else:
        datasets_for_archive = {
            UUID(dataset_id): exists
            for dataset_id, exists in zip(ids, index.datasets.bulk_has(ids))
        }

        if False in datasets_for_archive.values():
            for dataset_id, exists in datasets_for_archive.items():
                if not exists:
                    click.echo(f"No dataset found with id: {dataset_id}")
            sys.exit(-1)

        if archive_derived:
            derived_datasets = [
                _get_derived_set(index, dataset) for dataset in datasets_for_archive
            ]
            # Get the UUID of our found derived datasets
            derived_dataset_ids = [
                derived.id
                for derived_dataset in derived_datasets
                for derived in derived_dataset
            ]

    all_datasets = derived_dataset_ids + list(datasets_for_archive.keys())

    for dataset in all_datasets:
        click.echo(f"Archiving dataset: {dataset}")

    if not dry_run:
        index.datasets.archive(all_datasets)

    click.echo("Completed dataset archival.")


@dataset_cmd.command("restore", help="Restore datasets")
@click.option(
    "--restore-derived",
    "-d",
    help="Also recursively restore derived datasets",
    is_flag=True,
    default=False,
)
@click.option(
    "--dry-run",
    help="Don't restore. Display datasets that would get restored",
    is_flag=True,
    default=False,
)
@click.option(
    "--derived-tolerance-seconds",
    help="Only restore derived datasets that were archived "
    "this recently to the original dataset",
    default=10 * 60,
)
@click.option(
    "--all",
    "all_ds",
    help="Ignore id list - restore ALL archived datasets  (warning: may be slow on large databases)",
    is_flag=True,
    default=False,
)
@click.argument("ids", nargs=-1)
@ui.pass_index()
def restore_cmd(
    index: Index,
    restore_derived: bool,
    derived_tolerance_seconds: int,
    dry_run: bool,
    all_ds: bool,
    ids: list[str],
) -> None:
    if not ids and not all_ds:
        click.echo("Error: no datasets provided\n")
        print_help_msg(restore_cmd)
        sys.exit(1)

    tolerance = datetime.timedelta(seconds=derived_tolerance_seconds)
    if all_ds:
        ids = index.datasets.get_all_dataset_ids(archived=True)  # type: ignore[assignment]

    for id_ in ids:
        target_dataset = index.datasets.get(id_)
        if target_dataset is None:
            echo(f"No dataset found with id {id_}")
            sys.exit(-1)

        to_process = (
            _get_derived_set(index, UUID(id_)) if restore_derived else {target_dataset}
        )
        _LOG.debug("%s selected", len(to_process))

        # Only the already-archived ones.
        to_process = {d for d in to_process if d.is_archived}
        _LOG.debug("%s selected are archived", len(to_process))

        def within_tolerance(dataset):
            if not dataset.is_archived:
                return False
            t = target_dataset.archived_time
            return (t - tolerance) <= dataset.archived_time <= (t + tolerance)

        # Only those archived around the same time as the target.
        if restore_derived and target_dataset.is_archived:
            to_process = set(filter(within_tolerance, to_process))
            _LOG.debug(
                "%s selected were archived within the tolerance", len(to_process)
            )

        for d in to_process:
            click.echo(f"restoring {d.product.name} {d.id} {d.local_uri}")
        if not dry_run:
            index.datasets.restore(d.id for d in to_process)


@dataset_cmd.command("purge", help="Purge archived datasets")
@click.option(
    "--dry-run",
    help="Don't archive. Display datasets that would get archived",
    is_flag=True,
    default=False,
)
@click.option(
    "--all",
    "all_ds",
    help="Ignore id list - purge ALL archived datasets  (warning: may be slow on large databases)",
    is_flag=True,
    default=False,
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Allow active datasets to be deleted (default: false)",
)
@click.argument("ids", nargs=-1)
@ui.pass_index()
def purge_cmd(
    index: Index, dry_run: bool, all_ds: bool, force: bool, ids: list[str]
) -> None:
    if not ids and not all_ds:
        click.echo("Error: no datasets provided\n")
        print_help_msg(purge_cmd)
        sys.exit(1)

    if all_ds:
        datasets_for_purge = dict.fromkeys(
            index.datasets.get_all_dataset_ids(archived=True), True
        )
    else:
        datasets_for_purge = {
            UUID(dataset_id): exists
            for dataset_id, exists in zip(ids, index.datasets.bulk_has(ids))
        }

        # Check for non-existent datasets
        if False in datasets_for_purge.values():
            for dataset_id, exists in datasets_for_purge.items():
                if not exists:
                    click.echo(f"No dataset found with id: {dataset_id}")
            sys.exit(-1)

    if sys.stdin.isatty() and force:
        click.confirm(
            "Warning: you may be deleting active datasets. Proceed?", abort=True
        )

    if not dry_run:
        # Perform purge
        purged = index.datasets.purge(datasets_for_purge.keys(), force)
        not_purged = set(datasets_for_purge.keys()).difference(set(purged))
        if not force and not_purged:
            click.echo(
                "The following datasets are still active and could not be purged: "
                f"{', '.join([str(id_) for id_ in not_purged])}\n"
                "Use the --force option to delete anyway."
            )
        click.echo(f"{len(purged)} of {len(datasets_for_purge)} datasets purged")
    else:
        click.echo(f"{len(datasets_for_purge)} datasets not purged (dry run)")

    click.echo("Completed dataset purge.")


@dataset_cmd.command("find-duplicates", help="Search for duplicate indexed datasets")
@click.option(
    "--product",
    "-p",
    "product_names",
    help=(
        "Only search within product(s) specified with this option. "
        "You can supply several by repeating this option with a new product name."
    ),
    multiple=True,
)
@click.argument("fields", nargs=-1)
@ui.pass_index()
def find_duplicates(
    index: Index, product_names: Sequence[str], fields: Iterable[str]
) -> None:
    """
    Find dataset ids of two or more active datasets that have duplicate values in the specified fields.
    If products are specified, search only within those products. Otherwise, search within any products that
    have the fields.
    """
    if not fields:
        click.echo("Error: must provide field names to match on\n")
        sys.exit(1)

    # if no products were specified, use whichever ones have the specified search fields
    # if products were specified, check they all have the required fields
    products_with_fields = list(index.products.get_with_fields(fields))
    if not products_with_fields:
        click.echo(f"Error: no products found with fields {', '.join(fields)}\n")
        sys.exit(1)
    if not list(product_names):
        products = products_with_fields
    else:
        products = [
            p
            for p in (index.products.get_by_name(name) for name in product_names)
            if p is not None
        ]
        products_without_fields = set(products).difference(set(products_with_fields))
        if len(products_without_fields):
            click.echo(
                f"Error: specified products {', '.join(p.name for p in products_without_fields)} "
                "do not contain all required fields\n"
            )
            sys.exit(1)

    dupes = []
    for product in products:
        dupes.extend(list(index.datasets.search_product_duplicates(product, *fields)))
    if len(dupes):
        print(dupes)
    else:
        click.echo("No potential duplicates found.")
