from __future__ import absolute_import

import logging
import click

from pathlib import Path

from datacube.compat import string_types
from datacube.ui import click as ui
from datacube.utils import read_documents
from datacube.ui.common import get_metadata_path
from datacube.ui.click import cli
from datacube.model import Dataset

_LOG = logging.getLogger('datacube-dataset')


@cli.group(name='dataset', help='Dataset management commands')
def dataset_cmd():
    pass


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
    sources = {cls: match_dataset(source_doc, None, rules)
               for cls, source_doc in rule['type'].dataset_reader(dataset_doc).sources.items()}
    return Dataset(rule['type'], dataset_doc, uri, sources=sources)


def load_rules_from_file(filename, index):
    rules = next(read_documents(Path(filename)))[1]
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

    return rules


def load_rules_from_types(index, type_names=None):
    types = []
    if type_names:
        for name in type_names:
            type_ = index.datasets.types.get_by_name(name)
            if not type_:
                _LOG.error('DatasetType %s does not exists', name)
                return
            types += type_
    else:
        types += index.datasets.types.get_all()

    rules = [{'type': type_, 'metadata': type_.metadata} for type_ in types]
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
                _LOG.error('Unable to create Dataset for %s: %s', uri, e)
                continue

            if not check_dataset_consistent(dataset):
                _LOG.error("Dataset measurements don't match it's type specification %s", dataset.id)
                continue

            _LOG.info('Matched %s', dataset)
            if not dry_run:
                index.datasets.add(dataset)
