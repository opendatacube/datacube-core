from __future__ import absolute_import

import logging
import sys

import csv
import click
from click import echo
import datetime
import yaml
from pathlib import Path

from datacube.model import Dataset
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.ui.common import get_metadata_path
from datacube.utils import read_documents, changes

_LOG = logging.getLogger('datacube-dataset')


@cli.group(name='dataset', help='Dataset management commands')
def dataset_cmd():
    pass


def find_matching_product(rules, doc):
    """:rtype: datacube.model.DatasetType"""
    matched = [rule for rule in rules if changes.contains(doc, rule['metadata'])]
    if not matched:
        raise RuntimeError('No matching Product found for %s' % doc.get('id', 'unidentified'))
    if len(matched) > 1:
        raise RuntimeError('Too many matching Products found for %s. Matched %s.' % (
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


def create_dataset(dataset_doc, uri, rules):
    """
    :rtype datacube.model.Dataset:
    """
    dataset_type = find_matching_product(rules, dataset_doc)
    sources = {cls: create_dataset(source_doc, None, rules)
               for cls, source_doc in dataset_type.dataset_reader(dataset_doc).sources.items()}
    return Dataset(dataset_type, dataset_doc, uri, sources=sources)


def load_rules_from_file(filename, index):
    rules = next(read_documents(Path(filename)))[1]
    # TODO: verify schema

    for rule in rules:
        type_ = index.products.get_by_name(rule['type'])
        if not type_:
            _LOG.error('DatasetType %s does not exists', rule['type'])
            return
        if not changes.contains(type_.metadata_doc, rule['metadata']):
            _LOG.error('DatasetType %s can\'t be matched by its own rule', rule['type'])
            return
        rule['type'] = type_

    return rules


def load_rules_from_types(index, type_names=None):
    types = []
    if type_names:
        for name in type_names:
            type_ = index.products.get_by_name(name)
            if not type_:
                _LOG.error('DatasetType %s does not exists', name)
                return
            types.append(type_)
    else:
        types += index.products.get_all()

    rules = [{'type': type_, 'metadata': type_.metadata_doc} for type_ in types]
    return rules


def load_datasets(datasets, rules):
    for dataset_path in datasets:
        metadata_path = get_metadata_path(Path(dataset_path))
        if not metadata_path or not metadata_path.exists():
            _LOG.error('No supported metadata docs found for dataset %s', dataset_path)
            continue

        for metadata_path, metadata_doc in read_documents(metadata_path):
            uri = metadata_path.absolute().as_uri()

            try:
                dataset = create_dataset(metadata_doc, uri, rules)
            except RuntimeError as e:
                _LOG.exception("Error creating dataset")
                _LOG.error('Unable to create Dataset for %s: %s', uri, e)
                continue

            is_consistent, reason = check_dataset_consistent(dataset)
            if not is_consistent:
                _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)
                continue

            yield dataset


def parse_match_rules_options(index, match_rules, dtype, auto_match):
    if not (match_rules or dtype or auto_match):
        auto_match = True

    if match_rules:
        return load_rules_from_file(match_rules, index)
    else:
        assert dtype or auto_match
        return load_rules_from_types(index, dtype)


@dataset_cmd.command('add', help="Add datasets to the Data Cube")
@click.option('--match-rules', '-r', help='Rules to be used to associate datasets with products',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
@click.option('--dtype', '-t', help='Product to be associated with the datasets',
              multiple=True)
@click.option('--auto-match', '-a', help="Automatically associate datasets with products by matching metadata",
              is_flag=True, default=False)
@click.option('--dry-run', help='Check if everything is ok', is_flag=True, default=False)
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@ui.pass_index()
def index_cmd(index, match_rules, dtype, auto_match, dry_run, datasets):
    rules = parse_match_rules_options(index, match_rules, dtype, auto_match)
    if rules is None:
        return

    with click.progressbar(load_datasets(datasets, rules), label='Indexing datasets') as loadable_datasets:
        for dataset in loadable_datasets:
            _LOG.info('Matched %s', dataset)
            if not dry_run:
                index.datasets.add(dataset)


def parse_update_rules(allow_any):
    updates = {}
    for key_str in allow_any:
        updates[tuple(key_str.split('.'))] = changes.allow_any
    return updates


@dataset_cmd.command('update', help="Update datasets in the Data Cube")
@click.option('--allow-any', help="Allow any changes to the specified key (a.b.c)", multiple=True)
@click.option('--match-rules', '-r', help='Rules to be used to associate datasets with products',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
@click.option('--dtype', '-t', help='Product to be associated with the datasets', multiple=True)
@click.option('--auto-match', '-a', help="Automatically associate datasets with products by matching metadata",
              is_flag=True, default=False)
@click.option('--dry-run', help='Check if everything is ok', is_flag=True, default=False)
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@ui.pass_index()
def update_cmd(index, allow_any, match_rules, dtype, auto_match, dry_run, datasets):
    rules = parse_match_rules_options(index, match_rules, dtype, auto_match)
    if rules is None:
        return

    updates = parse_update_rules(allow_any)

    success, fail = 0, 0
    for dataset in load_datasets(datasets, rules):
        _LOG.info('Matched %s', dataset)

        if not dry_run:
            try:
                index.datasets.update(dataset, updates_allowed=updates)
                success += 1
                echo('Updated %s' % dataset.id)
            except ValueError as e:
                fail += 1
                echo('Failed to update %s: %s' % (dataset.id, e))
        else:
            if update_dry_run(index, updates, dataset):
                success += 1
            else:
                fail += 1
    echo('%d successful, %d failed' % (success, fail))


def update_dry_run(index, updates, dataset):
    try:
        can_update, safe_changes, unsafe_changes = index.datasets.can_update(dataset, updates_allowed=updates)
    except ValueError as e:
        echo('Cannot update %s: %s' % (dataset.id, e))
        return False

    for offset, old_val, new_val in safe_changes:
        echo('Safe change in %s:%s from %r to %r' % (dataset.id, '.'.join(offset), old_val, new_val))

    for offset, old_val, new_val in unsafe_changes:
        echo('Unsafe change in %s:%s from %r to %r' % (dataset.id, '.'.join(offset), old_val, new_val))

    if can_update:
        echo('Can update %s: %s unsafe changes, %s safe changes' % (dataset.id,
                                                                    len(unsafe_changes),
                                                                    len(safe_changes)))
    else:
        echo('Cannot update %s: %s unsafe changes, %s safe changes' % (dataset.id,
                                                                       len(unsafe_changes),
                                                                       len(safe_changes)))
    return can_update


def build_dataset_info(index, dataset, show_derived=False):
    deriveds = []
    if show_derived:
        deriveds = index.datasets.get_derived(dataset.id)

    # def find_me(derived):
    #     for key, source in derived.sources.items():
    #         print(dataset.id, source.id)
    #         if dataset.id == source.id:
    #             return key

    return {
        'id': dataset.id,
        'product': dataset.type.name,
        'location': dataset.local_uri,
        'sources': {key: build_dataset_info(index, source) for key, source in dataset.sources.items()},
        'derived': [build_dataset_info(index, derived) for derived in deriveds]
    }


@dataset_cmd.command('info', help="Display dataset id, product, location and provenance")
@click.option('--show-sources', help='Also show sources', is_flag=True, default=False)
@click.option('--show-derived', help='Also show sources', is_flag=True, default=False)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def info_cmd(index, show_sources, show_derived, ids):
    for id_ in ids:
        dataset = index.datasets.get(id_, include_sources=show_sources)
        if not dataset:
            click.echo('%s missing' % id_)
            continue

        yaml.safe_dump(build_dataset_info(index, dataset, show_derived), stream=sys.stdout)


def _write_csv(info):
    writer = csv.DictWriter(sys.stdout, ['id', 'product', 'location'], extrasaction='ignore')
    writer.writeheader()
    writer.writerows(info)


@dataset_cmd.command('search')
@click.option('-f', help='Output format', type=click.Choice(['yaml', 'csv']), default='csv', show_default=True)
@ui.parsed_search_expressions
@ui.pass_index()
def search_cmd(index, f, expressions):
    """
    Search available Datasets
    """
    datasets = index.datasets.search(**expressions)
    info = (build_dataset_info(index, dataset) for dataset in datasets)
    {
        'csv': _write_csv,
        'yaml': yaml.dump_all
    }[f](info)


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
              default=10*60)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def restore_cmd(index, restore_derived, derived_tolerance_seconds, dry_run, ids):
    tolerance = datetime.timedelta(seconds=derived_tolerance_seconds)

    for id_ in ids:
        _restore_one(dry_run, id_, index, restore_derived, tolerance)


def _restore_one(dry_run, id_, index, restore_derived, tolerance):
    """
    :type index: datacube.index._api.Index
    :type restore_derived: bool
    :type tolerance: datetime.timedelta
    :type dry_run:  bool
    :type id_: str
    """
    target_dataset = index.datasets.get(id_)
    to_process = _get_derived_set(index, id_) if restore_derived else {target_dataset}
    _LOG.debug("%s selected", len(to_process))

    # Only the already-archived ones.
    to_process = {d for d in to_process if d.archived_time is not None}
    _LOG.debug("%s selected are archived", len(to_process))

    def within_tolerance(dataset):
        if not dataset.archived_time:
            return False
        t = target_dataset.archived_time
        return (t - tolerance) <= dataset.archived_time <= (t + tolerance)

    # Only those archived around the same time as the target.
    if restore_derived and target_dataset.archived_time:
        to_process = set(filter(within_tolerance, to_process))
        _LOG.debug("%s selected were archived within the tolerance", len(to_process))

    for d in to_process:
        click.echo('restoring %s %s %s' % (d.type.name, d.id, d.local_uri))
    if not dry_run:
        index.datasets.restore(d.id for d in to_process)
