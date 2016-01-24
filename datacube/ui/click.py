# coding=utf-8
"""
Common functions for click-based cli scripts.
"""
from __future__ import absolute_import

import functools
import logging
import os

import click
import pkg_resources

from datacube import config
from datacube.index import index_connect


CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


def _print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    #: pylint: disable=not-callable
    version = pkg_resources.require('datacube')[0].version
    click.echo(
        '{prog}, version {version}'.format(
            prog='Data Cube',
            version=version
        )
    )
    ctx.exit()


def compose(*functions):
    """
    >>> compose(
    ...     lambda x: x+1,
    ...     lambda y: y+2
    ... )(1)
    4
    """

    def compose2(f, g):
        return lambda x: f(g(x))

    return functools.reduce(compose2, functions, lambda x: x)


def _init_logging(ctx, param, value):
    logging_level = logging.WARN - 10 * value
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging_level)
    logging.getLogger('datacube').setLevel(logging_level)


def _log_queries(ctx, param, value):
    if value:
        logging.getLogger('sqlalchemy.engine').setLevel('INFO')


def _set_config(ctx, param, value):
    if value:
        if not any(os.path.exists(p) for p in value):
            raise ValueError('No specified config paths exist: {}' % value)

        paths = value
    else:
        paths = config.DEFAULT_CONF_PATHS

    parsed_config = config.LocalConfig.find(paths=paths)

    if not ctx.obj:
        ctx.obj = {}

    ctx.obj['config_file'] = parsed_config


# This is a function, so it's valid to be lowercase.
#: pylint: disable=invalid-name
global_cli_options = compose(
    click.option('--version', is_flag=True,
                 callback=_print_version, expose_value=False, is_eager=True),
    click.option('--verbose', '-v', count=True, callback=_init_logging,
                 is_eager=True, expose_value=False,
                 help="Use multiple times for more verbosity"),
    click.option('--config_file', '-C', multiple=True, default='',
                 callback=_set_config, expose_value=False),
    click.option('--log-queries', is_flag=True, callback=_log_queries,
                 expose_value=False,
                 help="Print database queries.")
)


def pass_config(f):
    """Get a datacube config as the first argument. """

    def new_func(*args, **kwargs):
        config_ = click.get_current_context().obj['config_file']
        return f(config_, *args, **kwargs)

    return functools.update_wrapper(new_func, f)


def pass_index(f):
    """Get a connection to the index as the first argument. """

    def new_func(*args, **kwargs):
        index = index_connect(click.get_current_context().obj['config_file'])
        return f(index, *args, **kwargs)

    return functools.update_wrapper(new_func, f)
