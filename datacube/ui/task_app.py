from __future__ import absolute_import

import logging
import click
import cachetools
import functools
from itertools import chain
try:
    import cPickle as pickle
except ImportError:
    import pickle
from pathlib import Path

from datacube.ui import click as dc_ui
from datacube.utils import read_documents


_LOG = logging.getLogger('task-app')


@cachetools.cached(cache={}, key=lambda index, id_: id_)
def get_full_lineage(index, id_):
    return index.datasets.get(id_, include_sources=True)


def load_config(index, app_config_file, make_config, make_tasks, *args, **kwargs):
    app_config_path = Path(app_config_file)
    _, config = next(read_documents(app_config_path))
    config['app_config_file'] = app_config_path.name

    config = make_config(index, config, **kwargs)

    tasks = make_tasks(index, config, **kwargs)

    return config, iter(tasks)


def pickle_stream(objs, filename):
    idx = 0
    with open(filename, 'wb') as stream:
        for idx, obj in enumerate(objs, start=1):
            pickle.dump(obj, stream, pickle.HIGHEST_PROTOCOL)
    return idx


def unpickle_stream(filename):
    with open(filename, 'rb') as stream:
        while True:
            try:
                yield pickle.load(stream)
            except EOFError:
                break


def save_tasks(config, tasks, taskfile):
    i = pickle_stream(chain([config], tasks), taskfile)
    _LOG.info('Saved config and %d tasks to %s', i, taskfile)


def load_tasks(taskfile):
    stream = unpickle_stream(taskfile)
    config = next(stream)
    return config, stream


# This is a function, so it's valid to be lowercase.
#: pylint: disable=invalid-name
app_config_option = click.option('--app-config', help='App configuration file',
                                 type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
#: pylint: disable=invalid-name
load_tasks_option = click.option('--load-tasks', 'input_tasks_file', help='Load tasks from the specified file',
                                 type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
#: pylint: disable=invalid-name
save_tasks_option = click.option('--save-tasks', 'output_tasks_file', help='Save tasks to the specified file',
                                 type=click.Path(exists=False))

#: pylint: disable=invalid-name
task_app_options = dc_ui.compose(
    app_config_option,
    load_tasks_option,
    save_tasks_option,

    dc_ui.config_option,
    dc_ui.verbose_option,
    dc_ui.log_queries_option,
    dc_ui.executor_cli_options,
)


def task_app(make_config, make_tasks):
    """
    Create a `Task App` from a function

    Decorates a function
    :param make_config: callable(index, config, **query)
    :param make_tasks: callable(index, config, **kwargs)
    :return:
    """
    def decorate(app_func):
        def with_app_args(index, app_config=None, input_tasks_file=None, output_tasks_file=None, *args, **kwargs):
            if (app_config is None) == (input_tasks_file is None):
                click.echo('Must specify exactly one of --config, --load-tasks')
                click.get_current_context().exit(1)

            if app_config is not None:
                config, tasks = load_config(index, app_config, make_config, make_tasks, *args, **kwargs)

            if input_tasks_file:
                config, tasks = load_tasks(input_tasks_file)

            if output_tasks_file:
                save_tasks(config, tasks, output_tasks_file)
                return

            return app_func(index, config, tasks, *args, **kwargs)

        return functools.update_wrapper(with_app_args, app_func)

    return decorate


def check_existing_files(paths):
    """Check for existing files and optionally delete them.

    :param paths: sequence of path strings or path objects
    """
    click.echo('Files to be created:')
    existing_files = []
    total = 0
    for path in paths:
        total += 1
        file_path = Path(path)
        file_info = ''
        if file_path.exists():
            existing_files.append(file_path)
            file_info = ' - ALREADY EXISTS'
        click.echo('{}{}'.format(path, file_info))

    if existing_files:
        if click.confirm('There were {} existing files found that are not indexed. Delete those files now?'.format(
                len(existing_files))):
            for file_path in existing_files:
                file_path.unlink()

    click.echo('{total} tasks files to be created ({valid} valid files, {invalid} existing paths)'.format(
        total=total, valid=total - len(existing_files), invalid=len(existing_files)
    ))
