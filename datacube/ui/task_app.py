from __future__ import absolute_import

import logging
import click
import cachetools
import functools
try:
    import cPickle as pickle
except ImportError:
    import pickle
from pathlib import Path

from datacube.ui import click as dc_ui
from ..utils import read_documents


_LOG = logging.getLogger('task-app')


@cachetools.cached(cache={}, key=lambda index, id_: id_)
def get_full_lineage(index, id_):
    return index.datasets.get(id_, include_sources=True)


def config_loader(index, app_config_file, make_config, make_tasks, *args, **kwargs):
    app_config_path = Path(app_config_file)
    _, config = next(read_documents(app_config_path))
    config['app_config_file'] = app_config_path.name

    config = make_config(index, config, **kwargs)

    tasks = make_tasks(index, config, **kwargs)
    _LOG.info('%s tasks discovered', len(tasks))

    return config, tasks


def task_saver(config, tasks, taskfile):
    with open(taskfile, 'wb') as stream:
        pickler = pickle.Pickler(stream, pickle.HIGHEST_PROTOCOL)
        pickler.dump(config)
        for task in tasks:
            pickler.dump(task)
    _LOG.info('Saved config and tasks to %s', taskfile)


def stream_unpickler(taskfile):
    with open(taskfile, 'rb') as stream:
        unpickler = pickle.Unpickler(stream)
        while True:
            try:
                yield unpickler.load()
            except EOFError:
                break


def task_loader(index, taskfile):
    stream = stream_unpickler(taskfile)
    config = next(stream)
    return config, stream


# This is a function, so it's valid to be lowercase.
#: pylint: disable=invalid-name
app_config_option = click.option('--app-config', help='App configuration file',
                                 type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
#: pylint: disable=invalid-name
load_tasks_option = click.option('--load-tasks', help='Load tasks from the specified file',
                                 type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
#: pylint: disable=invalid-name
save_tasks_option = click.option('--save-tasks', help='Save tasks to the specified file',
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
        def with_app_args(index, app_config=None, load_tasks=None, save_tasks=None, *args, **kwargs):
            if (app_config is None) == (load_tasks is None):
                click.echo('Must specify exactly one of --config, --load-tasks')
                click.get_current_context().exit(1)

            if app_config:
                config, tasks = config_loader(index, app_config, make_config, make_tasks, *args, **kwargs)

            if load_tasks:
                config, tasks = task_loader(index, load_tasks)

            if save_tasks:
                task_saver(config, tasks, save_tasks)
                return

            return app_func(index, config, tasks, *args, **kwargs)

        return functools.update_wrapper(with_app_args, app_func)

    return decorate
