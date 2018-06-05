from __future__ import absolute_import

import csv
import datetime
import logging
import sys
from collections import OrderedDict
from pathlib import Path

import click
import yaml
import yaml.resolver
from click import echo
import json

from datacube.index.index import Index
from datacube.index.exceptions import MissingRecordError
from datacube.model import Dataset
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.ui.common import get_metadata_path
from datacube.utils import read_documents, changes, InvalidDocException, without_lineage_sources
from datacube.utils.serialise import SafeDatacubeDumper

try:
    from typing import Iterable
except ImportError:
    pass

_LOG = logging.getLogger('datacube-dataset')


class BadMatch(Exception):
    pass


def report_old_options(mapping):
    def maybe_remap(s):
        if s in mapping:
            _LOG.warning("DEPRECATED option detected: --%s use --%s instead", s, mapping[s])
            return mapping[s]
        else:
            return s
    return maybe_remap


@cli.group(name='dataset', help='Dataset management commands')
def dataset_cmd():
    pass


def find_matching_product(rules, doc):
    """:rtype: datacube.model.DatasetType"""
    matched = [rule for rule in rules if changes.contains(doc, rule['metadata'])]
    if not matched:
        # provide user with information about the failure
        if len(rules) == 0:
            raise BadMatch('No rules provided.')
        elif len(rules) == 1:
            metadata = rules[0]['metadata']
            relevant_doc = {k: v for k, v in doc.items() if k in metadata}
            raise BadMatch('Dataset metadata did not match product rules.'
                           '\nDataset metadata:\n %s\n'
                           '\nProduct metadata:\n %s\n'
                           % (json.dumps(relevant_doc, indent=4),
                              json.dumps(metadata, indent=4)))
        else:
            raise BadMatch('No matching Product found for %s' % json.dumps(doc, indent=4))
    if len(matched) > 1:
        raise BadMatch('Too many matching Products found for %s. Matched %s.' % (
            doc.get('id', 'unidentified'), matched))
    return matched[0]['type']


def check_dataset_consistent(dataset):
    """
    :type dataset: datacube.model.Dataset
    :return: (Is consistent, [error message|None])
    :rtype: (bool, str or None)
    """
    # It the type expects measurements, ensure our dataset contains them all.
    if not set(dataset.type.measurements.keys()).issubset(dataset.measurements.keys()):
        return False, "measurement fields don't match type specification"

    return True, None


def create_dataset(dataset_doc, uri, rules, skip_lineage=False):
    """
    :rtype datacube.model.Dataset:
    """
    dataset_type = find_matching_product(rules, dataset_doc)
    if skip_lineage:
        sources = {}
        dataset_doc = without_lineage_sources(dataset_doc, dataset_type)
    else:
        sources = {cls: create_dataset(source_doc, None, rules)
                   for cls, source_doc in dataset_type.dataset_reader(dataset_doc).sources.items()}
    return Dataset(dataset_type, dataset_doc, uris=[uri] if uri else None, sources=sources)


def load_rules_from_types(index, type_names=None):
    types = []
    if type_names:
        for name in type_names:
            type_ = index.products.get_by_name(name)
            if not type_:
                _LOG.error('DatasetType %s does not exists', name)
                return None
            types.append(type_)
    else:
        types += index.products.get_all()

    rules = [{'type': type_, 'metadata': type_.metadata_doc} for type_ in types]
    return rules


def load_datasets(datasets, rules, skip_lineage=False):
    for dataset_path in datasets:
        metadata_path = get_metadata_path(Path(dataset_path))
        if not metadata_path or not metadata_path.exists():
            _LOG.error('No supported metadata docs found for dataset %s', dataset_path)
            continue

        try:
            for uri, metadata_doc in read_documents(metadata_path, uri=True):
                try:
                    dataset = create_dataset(metadata_doc, uri, rules, skip_lineage=skip_lineage)
                except BadMatch as e:
                    _LOG.error('Unable to create Dataset for %s: %s', uri, e)
                    continue

                is_consistent, reason = check_dataset_consistent(dataset)
                if not is_consistent:
                    _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)
                    continue

                yield dataset
        except InvalidDocException:
            _LOG.error("Failed reading documents from %s", metadata_path)
            continue


def load_datasets_for_update(datasets, index):
    """Load datasets from disk, associate to a product by looking up existing
    dataset in the index. Datasets not in the database will be logged.

    Doesn't load lineage information

    Generates tuples in the form (new_dataset, existing_dataset)
    """
    def mk_dataset(metadata_doc, uri):
        uuid = metadata_doc.get('id', None)

        if uuid is None:
            return None, None, "Metadata document it missing id field"

        existing = index.datasets.get(uuid)
        if existing is None:
            return None, None, "No such dataset the database: {}".format(uuid)

        return Dataset(existing.type,
                       without_lineage_sources(metadata_doc, existing.type, inplace=True),
                       uris=[uri]), existing, None

    for dataset_path in datasets:
        metadata_path = get_metadata_path(Path(dataset_path))
        if not metadata_path or not metadata_path.exists():
            _LOG.error('No supported metadata docs found for dataset %s', dataset_path)
            continue

        try:
            for uri, metadata_doc in read_documents(metadata_path, uri=True):
                dataset, existing, error_msg = mk_dataset(metadata_doc, uri)

                if dataset is None:
                    _LOG.error("Failure while processing: %s\n" +
                               " > Reason: %s", metadata_path, error_msg)
                else:
                    is_consistent, reason = check_dataset_consistent(dataset)
                    if is_consistent:
                        yield dataset, existing
                    else:
                        _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)

        except InvalidDocException:
            _LOG.error("Failed reading documents from %s", metadata_path)
            continue


def parse_match_rules_options(index, dtype, auto_match):
    if auto_match is True:
        _LOG.warning("--auto-match option is deprecated, update your scripts, behaviour is the same without it")

    return load_rules_from_types(index, dtype)


@dataset_cmd.command('add',
                     help="Add datasets to the Data Cube",
                     context_settings=dict(token_normalize_func=report_old_options({
                         'dtype': 'product',
                         't': 'p'
                     })))
@click.option('--product', '-p', 'product_names',
              help=('Only match against products specified with this option, '
                    'you can supply several by repeating this option with a new product name'),
              multiple=True)
@click.option('--auto-match', '-a', help="Deprecated don't use it, it's a no-op",
              is_flag=True, default=False)
@click.option('--sources-policy', type=click.Choice(['verify', 'ensure', 'skip']), default='verify',
              help="""'verify' - verify source datasets' metadata (default)
'ensure' - add source dataset if it doesn't exist
'skip' - dont add the derived dataset if source dataset doesn't exist""")
@click.option('--dry-run', help='Check if everything is ok', is_flag=True, default=False)
@click.option('--ignore-lineage',
              help="Don't add lineage data to the database",
              is_flag=True, default=False)
@click.option('--confirm-ignore-lineage',
              help="Don't add lineage data to the database, without confirmation",
              is_flag=True, default=False)
@click.argument('dataset-paths',
                type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@ui.pass_index()
def index_cmd(index, product_names, auto_match, sources_policy, dry_run,
              ignore_lineage,
              confirm_ignore_lineage,
              dataset_paths):

    if confirm_ignore_lineage is False and ignore_lineage is True:
        if sys.stdin.isatty():
            confirmed = click.confirm("Requested to skip lineage information, Are you sure?", default=False)
            if not confirmed:
                click.echo('OK aborting', err=True)
                sys.exit(1)
        else:
            click.echo("Use --confirm-ignore-lineage from non-interactive scripts. Aborting.")
            sys.exit(1)

        confirm_ignore_lineage = True

    rules = parse_match_rules_options(index, product_names, auto_match)
    if rules is None:
        return

    # If outputting directly to terminal, show a progress bar.
    if sys.stdout.isatty():
        with click.progressbar(dataset_paths, label='Indexing datasets') as dataset_path_iter:
            index_dataset_paths(sources_policy, dry_run, index, rules, dataset_path_iter,
                                skip_lineage=confirm_ignore_lineage)
    else:
        index_dataset_paths(sources_policy, dry_run, index, rules, dataset_paths,
                            skip_lineage=confirm_ignore_lineage)


def index_dataset_paths(sources_policy, dry_run, index, rules, dataset_paths, skip_lineage=False):
    for dataset in load_datasets(dataset_paths, rules, skip_lineage=skip_lineage):
        _LOG.info('Matched %s', dataset)
        if not dry_run:
            try:
                index.datasets.add(dataset, sources_policy=sources_policy)
            except (ValueError, MissingRecordError) as e:
                _LOG.error('Failed to add dataset %s: %s', dataset.local_uri, e)


def parse_update_rules(keys_that_can_change):
    updates_allowed = {}
    for key_str in keys_that_can_change:
        updates_allowed[tuple(key_str.split('.'))] = changes.allow_any
    return updates_allowed


@dataset_cmd.command('update', help="Update datasets in the Data Cube")
@click.option('--allow-any', 'keys_that_can_change',
              help="Allow any changes to the specified key (a.b.c)",
              multiple=True)
@click.option('--dry-run', help='Check if everything is ok', is_flag=True, default=False)
@click.option('--location-policy',
              type=click.Choice(['keep', 'archive', 'forget']),
              default='keep',
              help='''What to do with previously recorded dataset location
'keep' - keep as alternative location [default]
'archive' - mark as archived
'forget' - remove from the index
''')
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@ui.pass_index()
def update_cmd(index, keys_that_can_change, dry_run, location_policy, datasets):

    def loc_action(action, new_ds, existing_ds, action_name):
        if len(existing_ds.uris) == 0:
            return None

        if len(existing_ds.uris) > 1:
            _LOG.warning("Refusing to %s old location, there are several", action_name)
            return None

        new_uri, = new_ds.uris
        old_uri, = existing_ds.uris

        if new_uri == old_uri:
            return None

        if dry_run:
            echo('Will {} old location {}, and add new one {}'.format(action_name, old_uri, new_uri))
            return True

        return action(existing_ds.id, old_uri)

    def loc_archive(new_ds, existing_ds):
        return loc_action(index.datasets.archive_location, new_ds, existing_ds, 'archive')

    def loc_forget(new_ds, existing_ds):
        return loc_action(index.datasets.remove_location, new_ds, existing_ds, 'forget')

    def loc_keep(new_ds, existing_ds):
        return None

    update_loc = dict(archive=loc_archive,
                      forget=loc_forget,
                      keep=loc_keep)[location_policy]

    updates_allowed = parse_update_rules(keys_that_can_change)

    success, fail = 0, 0
    for dataset, existing_ds in load_datasets_for_update(datasets, index):
        _LOG.info('Matched %s', dataset)

        if location_policy != 'keep':
            if len(existing_ds.uris) > 1:
                # TODO:
                pass

        if not dry_run:
            try:
                index.datasets.update(dataset, updates_allowed=updates_allowed)
                update_loc(dataset, existing_ds)
                success += 1
                echo('Updated %s' % dataset.id)
            except ValueError as e:
                fail += 1
                echo('Failed to update %s: %s' % (dataset.id, e))
        else:
            if update_dry_run(index, updates_allowed, dataset):
                update_loc(dataset, existing_ds)
                success += 1
            else:
                fail += 1
    echo('%d successful, %d failed' % (success, fail))


def update_dry_run(index, updates_allowed, dataset):
    try:
        can_update, safe_changes, unsafe_changes = index.datasets.can_update(dataset, updates_allowed=updates_allowed)
    except ValueError as e:
        echo('Cannot update %s: %s' % (dataset.id, e))
        return False

    if can_update:
        echo('Can update %s: %s unsafe changes, %s safe changes' % (dataset.id,
                                                                    len(unsafe_changes),
                                                                    len(safe_changes)))
    else:
        echo('Cannot update %s: %s unsafe changes, %s safe changes' % (dataset.id,
                                                                       len(unsafe_changes),
                                                                       len(safe_changes)))
    return can_update


def build_dataset_info(index, dataset, show_sources=False, show_derived=False, depth=1, max_depth=99):
    # type: (Index, Dataset, bool) -> dict

    info = OrderedDict((
        ('id', str(dataset.id)),
        ('product', dataset.type.name),
        ('status', 'archived' if dataset.is_archived else 'active')
    ))

    # Optional when loading a dataset.
    if dataset.indexed_time is not None:
        info['indexed'] = dataset.indexed_time

    info['locations'] = dataset.uris
    info['fields'] = dataset.metadata.search_fields

    if depth < max_depth:
        if show_sources:
            info['sources'] = {key: build_dataset_info(index, source,
                                                       show_sources=True, show_derived=False,
                                                       depth=depth + 1, max_depth=max_depth)
                               for key, source in dataset.sources.items()}

        if show_derived:
            info['derived'] = [build_dataset_info(index, derived,
                                                  show_sources=False, show_derived=True,
                                                  depth=depth + 1, max_depth=max_depth)
                               for derived in index.datasets.get_derived(dataset.id)]

    return info


def _write_csv(infos):
    writer = csv.DictWriter(sys.stdout, ['id', 'status', 'product', 'location'], extrasaction='ignore')
    writer.writeheader()

    def add_first_location(row):
        locations_ = row['locations']
        row['location'] = locations_[0] if locations_ else None
        return row

    writer.writerows(add_first_location(row) for row in infos)


def _write_yaml(infos):
    """
    Dump yaml data with support for OrderedDicts.

    Allows for better human-readability of output: such as dataset ID field first, sources last.

    (Ordered dicts are output identically to normal yaml dicts: their order is purely for readability)
    """

    return yaml.dump_all(infos, sys.stdout, SafeDatacubeDumper, default_flow_style=False, indent=4)


_OUTPUT_WRITERS = {
    'csv': _write_csv,
    'yaml': _write_yaml,
}


@dataset_cmd.command('info', help="Display dataset information")
@click.option('--show-sources', help='Also show source datasets', is_flag=True, default=False)
@click.option('--show-derived', help='Also show derived datasets', is_flag=True, default=False)
@click.option('-f', help='Output format',
              type=click.Choice(list(_OUTPUT_WRITERS)), default='yaml', show_default=True)
@click.option('--max-depth',
              help='Maximum sources/derived depth to travel',
              type=int,
              # Unlikely to be hit, but will avoid total-death by circular-references.
              default=99)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def info_cmd(index, show_sources, show_derived, f, max_depth, ids):
    # type: (Index, bool, bool, Iterable[str]) -> None

    # Using an array wrapper to get around the lack of "nonlocal" in py2
    missing_datasets = [0]

    def get_datasets(ids):
        for id_ in ids:
            dataset = index.datasets.get(id_, include_sources=show_sources)
            if dataset:
                yield dataset
            else:
                click.echo('%s missing' % id_, err=True)
                missing_datasets[0] += 1

    _OUTPUT_WRITERS[f](
        build_dataset_info(index,
                           dataset,
                           show_sources=show_sources,
                           show_derived=show_derived,
                           max_depth=max_depth)
        for dataset in get_datasets(ids)
    )
    sys.exit(missing_datasets[0])


@dataset_cmd.command('search')
@click.option('--limit', help='Limit the number of results',
              type=int, default=None)
@click.option('-f', help='Output format',
              type=click.Choice(list(_OUTPUT_WRITERS)), default='yaml', show_default=True)
@ui.parsed_search_expressions
@ui.pass_index()
def search_cmd(index, limit, f, expressions):
    """
    Search available Datasets
    """
    datasets = index.datasets.search(limit=limit, **expressions)
    _OUTPUT_WRITERS[f](
        build_dataset_info(index, dataset)
        for dataset in datasets
    )


def _get_derived_set(index, id_):
    """
    Get a single flat set of all derived datasets.
    (children, grandchildren, great-grandchildren...)
    """
    derived_set = {index.datasets.get(id_)}
    to_process = {id_}
    while to_process:
        derived = index.datasets.get_derived(to_process.pop())
        to_process.update(d.id for d in derived)
        derived_set.update(derived)
    return derived_set


@dataset_cmd.command('archive', help="Archive datasets")
@click.option('--archive-derived', '-d', help='Also recursively archive derived datasets', is_flag=True, default=False)
@click.option('--dry-run', help="Don't archive. Display datasets that would get archived",
              is_flag=True, default=False)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def archive_cmd(index, archive_derived, dry_run, ids):
    for id_ in ids:
        to_process = _get_derived_set(index, id_) if archive_derived else [index.datasets.get(id_)]
        for d in to_process:
            click.echo('archiving %s %s %s' % (d.type.name, d.id, d.local_uri))
        if not dry_run:
            index.datasets.archive(d.id for d in to_process)


@dataset_cmd.command('restore', help="Restore datasets")
@click.option('--restore-derived', '-d', help='Also recursively restore derived datasets', is_flag=True, default=False)
@click.option('--dry-run', help="Don't restore. Display datasets that would get restored",
              is_flag=True, default=False)
@click.option('--derived-tolerance-seconds',
              help="Only restore derived datasets that were archived "
                   "this recently to the original dataset",
              default=10 * 60)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def restore_cmd(index, restore_derived, derived_tolerance_seconds, dry_run, ids):
    tolerance = datetime.timedelta(seconds=derived_tolerance_seconds)

    for id_ in ids:
        _restore_one(dry_run, id_, index, restore_derived, tolerance)


def _restore_one(dry_run, id_, index, restore_derived, tolerance):
    """
    :type index: datacube.index.index.Index
    :type restore_derived: bool
    :type tolerance: datetime.timedelta
    :type dry_run:  bool
    :type id_: str
    """
    target_dataset = index.datasets.get(id_)
    to_process = _get_derived_set(index, id_) if restore_derived else {target_dataset}
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
        _LOG.debug("%s selected were archived within the tolerance", len(to_process))

    for d in to_process:
        click.echo('restoring %s %s %s' % (d.type.name, d.id, d.local_uri))
    if not dry_run:
        index.datasets.restore(d.id for d in to_process)
