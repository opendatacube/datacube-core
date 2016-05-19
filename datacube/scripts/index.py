from __future__ import absolute_import

import logging
import click

from pathlib import Path

from datacube.compat import string_types
from datacube.ui import click as ui
from datacube.ui import read_documents
from datacube.ui.common import get_metadata_path
from datacube.ui.click import cli
from datacube.model import Dataset

_LOG = logging.getLogger('agdc-index')


def contains(v1, v2):
    """
    Check that v1 contains v2

    For dicts contains(v1[k], v2[k]) for all k in v2
    For other types v1 == v2

    >>> contains("bob", "BOB")
    True
    >>> contains({'a':1, 'b': 2}, {'a':1})
    True
    >>> contains({'a':{'b': 'BOB'}}, {'a':{'b': 'bob'}})
    True
    >>> contains("bob", "alice")
    False
    >>> contains({'a':1}, {'a':1, 'b': 2})
    False
    """
    if isinstance(v1, string_types):
        return isinstance(v2, string_types) and v1.lower() == v2.lower()

    if isinstance(v1, dict):
        return isinstance(v2, dict) and all(contains(v1.get(k, object()), v) for k, v in v2.items())

    return v1 == v2


def match_doc(rules, doc):
    matched = [rule for rule in rules if contains(doc, rule['metadata'])]
    if not matched:
        raise RuntimeError('No matches found for %s' % doc.get('id', 'unidentified'))
    if len(matched) > 1:
        raise RuntimeError('Too many matches found for' % doc.get('id', 'unidentified'))
    return matched[0]


def check_dataset_consistent(dataset):
    return set(dataset.type.measurements.keys()).issubset(dataset.measurements.keys())


def match_dataset(dataset_doc, uri, rules):
    """
    :rtype datacube.model.Dataset:
    """
    rule = match_doc(rules, dataset_doc)
    dataset = Dataset(rule['type'], dataset_doc, uri, managed=rule.get('managed', False))
    dataset.sources = {cls: match_dataset(source_doc, None, rules)
                       for cls, source_doc in dataset.metadata.sources.items()}
    return dataset


def match_madness(match_rules, index):
    rules = next(read_documents(Path(match_rules)))[1]
    # TODO: verify schema

    for rule in rules:
        type_ = index.datasets.types.get_by_name(rule['type'])
        if not type_:
            _LOG.error('DatasetType %s does not exists', rule['type'])
            return
        if not contains(type_.metadata, rule['metadata']):
            _LOG.error('DatasetType %s can\'t be matched by its own rule', rule['type'])
            return
        rule['type'] = type_

    def generate_dataset(metadata_doc, uri):
        try:
            return match_dataset(metadata_doc, uri, rules)
        except RuntimeError as e:
            _LOG.error('Unable to create Dataset for %s: %s', uri, e)

    return generate_dataset


def type_crazy(type_name, managed, index):
    type_ = index.datasets.types.get_by_name(type_name)
    if not type_:
        _LOG.error("DatasetType %s does not exist", type_name)
        return

    def generate_dataset(metadata_doc, uri):
        if not contains(metadata_doc, type_.metadata):
            _LOG.warning('Dataset %s does match the specified type %s',
                         metadata_doc.get('id', 'unidentified'), type_.name)

        dataset = Dataset(type_, metadata_doc, uri, managed=managed)
        if dataset.metadata.sources:
            _LOG.error('Dataset %s contains source datasets, that can\'t be matched', dataset.id)
            return None

        return dataset

    return generate_dataset


@cli.command('index', help="Index datasets into the Data Cube")
@click.option('--match-rules', '-r',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='Rules to be used to find dataset types for datasets')
@click.option('--dtype', '-t',
              help='Dataset Type to be used to index datasets')
@click.option('--managed', '-m', is_flag=True, default=False,
              help='Should Data Cube manage dataset\'s files')
@click.option('--dry-run', '-d', is_flag=True, default=False, help='Check if everything is ok')
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index(app_name='agdc-index')
def index_cmd(index, match_rules, dtype, managed, dry_run, datasets):
    if match_rules is None is dtype is None:
        _LOG.error('Must specify one of [--match-rules, --type]')
        return

    generate_dataset = None
    if match_rules:
        generate_dataset = match_madness(match_rules, index)
    if dtype:
        generate_dataset = type_crazy(dtype, managed, index)
    if generate_dataset is None:
        return

    for dataset_path in datasets:
        metadata_path = get_metadata_path(Path(dataset_path))
        if not metadata_path or not metadata_path.exists():
            raise ValueError('No supported metadata docs found for dataset {}'.format(dataset_path))

        for metadata_path, metadata_doc in read_documents(metadata_path):
            uri = metadata_path.absolute().as_uri()
            dataset = generate_dataset(metadata_doc, uri)

            if not check_dataset_consistent(dataset):
                _LOG.error("Dataset measurements don't match it's type specification %s", dataset.id)
                continue

            _LOG.info('Matched %s', dataset)
            if not dry_run:
                index.datasets.add(dataset)
