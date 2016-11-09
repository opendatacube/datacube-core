from __future__ import absolute_import

import logging
import sys

import csv
import click
import yaml
from pathlib import Path

from datacube.model import Dataset
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.ui.common import get_metadata_path
from datacube.utils import read_documents, contains

_LOG = logging.getLogger('datacube-dataset')


@cli.group(name='dataset', help='Dataset management commands')
def dataset_cmd():
    pass


def match_doc(rules, doc):
    matched = [rule for rule in rules if contains(doc, rule['metadata'])]
    if not matched:
        raise RuntimeError('No matches found for %s' % doc.get('id', 'unidentified'))
    if len(matched) > 1:
        raise RuntimeError('Too many matches found for' % doc.get('id', 'unidentified'))
    return matched[0]


def check_dataset_consistent(dataset):
    """
    :type dataset: datacube.model.Dataset
    :return: (Is consistent, error message)
    :rtype: (bool, str or None)
    """
    # It the type expects measurements, ensure our dataset contains them all.
    if not set(dataset.type.measurements.keys()).issubset(dataset.measurements.keys()):
        return False, "measurement fields don't match type specification"

    return True, None


def match_dataset(dataset_doc, uri, rules):
    """
    :rtype datacube.model.Dataset:
    """
    rule = match_doc(rules, dataset_doc)
    sources = {cls: match_dataset(source_doc, None, rules)
               for cls, source_doc in rule['type'].dataset_reader(dataset_doc).sources.items()}
    return Dataset(rule['type'], dataset_doc, uri, sources=sources)


def load_rules_from_file(filename, index):
    rules = next(read_documents(Path(filename)))[1]
    # TODO: verify schema

    for rule in rules:
        type_ = index.products.get_by_name(rule['type'])
        if not type_:
            _LOG.error('DatasetType %s does not exists', rule['type'])
            return
        if not contains(type_.metadata_doc, rule['metadata']):
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
    if not (match_rules or dtype or auto_match):
        _LOG.error('Must specify one of [--match-rules, --type, --auto-match]')
        return

    if match_rules:
        rules = load_rules_from_file(match_rules, index)
    else:
        assert dtype or auto_match
        rules = load_rules_from_types(index, dtype)

    if rules is None:
        return

    for dataset_path in datasets:
        metadata_path = get_metadata_path(Path(dataset_path))
        if not metadata_path or not metadata_path.exists():
            raise ValueError('No supported metadata docs found for dataset {}'.format(dataset_path))

        for metadata_path, metadata_doc in read_documents(metadata_path):
            uri = metadata_path.absolute().as_uri()

            try:
                dataset = match_dataset(metadata_doc, uri, rules)
            except RuntimeError as e:
                _LOG.exception("Error creating dataset")
                _LOG.error('Unable to create Dataset for %s: %s', uri, e)
                continue

            is_consistent, reason = check_dataset_consistent(dataset)
            if not is_consistent:
                _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)
                continue

            _LOG.info('Matched %s', dataset)
            if not dry_run:
                index.datasets.add(dataset)


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


@dataset_cmd.command('search', help="Search datasets")
@click.option('-f', help='Output format', type=click.Choice(['yaml', 'csv']), default='csv', show_default=True)
@ui.parsed_search_expressions
@ui.pass_index()
def search_cmd(index, f, expressions):
    datasets = index.datasets.search(**expressions)
    info = (build_dataset_info(index, dataset) for dataset in datasets)
    {
        'csv': _write_csv,
        'yaml': yaml.dump_all
    }[f](info)


def get_derived_set(index, id_):
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
        to_process = get_derived_set(index, id_) if archive_derived else [index.datasets.get(id_)]
        for d in to_process:
            click.echo('archiving %s %s %s' % (d.type.name, d.id, d.local_uri))
        if not dry_run:
            index.datasets.archive(d.id for d in to_process)


@dataset_cmd.command('restore', help="Restore datasets")
@click.option('--restore-derived', '-d', help='Also recursively restore derived datasets', is_flag=True, default=False)
@click.option('--dry-run', help="Don't restore. Display datasets that would get restored",
              is_flag=True, default=False)
@click.argument('ids', nargs=-1)
@ui.pass_index()
def restore_cmd(index, restore_derived, dry_run, ids):
    for id_ in ids:
        to_process = get_derived_set(index, id_) if restore_derived else [index.datasets.get(id_)]
        for d in to_process:
            click.echo('restoring %s %s %s' % (d.type.name, d.id, d.local_uri))
        if not dry_run:
            index.datasets.restore(d.id for d in to_process)
