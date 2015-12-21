# coding=utf-8
"""
Module
"""
from __future__ import absolute_import, print_function

import logging
from pathlib import Path

from click.testing import CliRunner

import datacube.scripts.config_tool

_LOG = logging.getLogger(__name__)

MAPPING_EXAMPLE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples').glob('**/*mapping.yaml')


def test_add_example_mapping_docs(global_integration_cli_args, db):
    """
    Add example mapping docs, to ensure they're valid and up-to-date.
    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    existing_mappings = db.count_mappings()
    print('{} mappings'.format(existing_mappings))
    for mapping_path in MAPPING_EXAMPLE_DOCS:
        print('Adding mapping {}'.format(mapping_path))
        opts = list(global_integration_cli_args)
        opts.extend(
            [
                '-v', 'mappings', 'add',
                str(mapping_path)
            ]
        )
        print(opts)
        runner = CliRunner()
        result = runner.invoke(
            datacube.scripts.config_tool.cli,
            opts
        )
        print(result.output)
        assert result.exit_code == 0
        mappings_count = db.count_mappings()
        assert mappings_count > existing_mappings, "Mapping document was not added: " + str(mapping_path)
        existing_mappings = mappings_count
