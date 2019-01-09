
import logging
import os
import time
import click
import cachetools
import functools
import itertools
import re
from pathlib import Path
import pandas as pd
import pickle

from datacube.ui import click as dc_ui
from datacube.utils import read_documents


_LOG = logging.getLogger(__name__)


@cachetools.cached(cache={}, key=lambda index, id_: id_)
def get_full_lineage(index, id_):
    return index.datasets.get(id_, include_sources=True)


def load_config(index, app_config_file, make_config, make_tasks, *args, **kwargs):
    app_config_path = Path(app_config_file)
    _, config = next(read_documents(app_config_path))
    config['app_config_file'] = app_config_path.name
    config['task_timestamp'] = int(time.time())

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
    """Saves the config

    :param config: dict of configuration options common to all tasks
    :param tasks:
    :param str taskfile: Name of output file
    :return: Number of tasks saved to the file
    """
    i = pickle_stream(itertools.chain([config], tasks), taskfile)
    if i <= 1:
        # Only saved the config, no tasks!
        os.remove(taskfile)
        return 0
    else:
        _LOG.info('Saved config and %d tasks to %s', i - 1, taskfile)
    return i - 1


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
queue_size_option = click.option('--queue-size', help='Number of tasks to queue at the start',
                                 type=click.IntRange(1, 100000), default=3200)

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


def _cell_list_from_file(filename):
    cell_matcher = re.compile(r'(-?\d+)(?:\s*(?:,|_|\s)\s*)(-?\d+)')
    with open(filename) as cell_file:
        for line in cell_file:
            match = cell_matcher.match(line)
            if match:
                yield tuple(int(i) for i in match.groups())


def cell_list_to_file(filename, cell_list):
    with open(filename, 'w') as cell_file:
        for cell in cell_list:
            cell_file.write('{0},{1}\n'.format(*cell))


def validate_cell_list(ctx, param, value):
    try:
        if value is None:
            return None
        return list(_cell_list_from_file(value))
    except ValueError:
        raise click.BadParameter('cell_index_list must be a file with lines in the form "14,-11"')


def validate_cell_index(ctx, param, value):
    try:
        if value is None:
            return None
        return tuple(int(i) for i in value.split(',', 2))
    except ValueError:
        raise click.BadParameter('cell_index must be specified in the form "14,-11"')


def validate_year(ctx, param, value):
    try:
        if value is None:
            return None
        years = [pd.Period(y) for y in value.split('-', 2)]
        return years[0].start_time.to_pydatetime(warn=False), years[-1].end_time.to_pydatetime(warn=False)
    except ValueError:
        raise click.BadParameter('year must be specified as a single year (eg 1996) '
                                 'or as an inclusive range (eg 1996-2001)')


def break_query_into_years(time_query, **kwargs):
    if time_query is None:
        return [kwargs]
    return [dict(time=time_range, **kwargs) for time_range in year_splitter(*time_query)]


def year_splitter(start, end):
    """
    Produces a list of time ranges based that represent each year in the range.

    `year_splitter('1992', '1993')` returns:

    ::
        [('1992-01-01 00:00:00', '1992-12-31 23:59:59.9999999'),
         ('1993-01-01 00:00:00', '1993-12-31 23:59:59.9999999')]

    :param str start: start year
    :param str end: end year
    :return Generator[tuple(str, str)]: strings representing the ranges
    """
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    for p in pd.period_range(start=start_ts, end=end_ts, freq='A'):
        yield str(p.start_time), str(p.end_time)


#: pylint: disable=invalid-name
cell_index_option = click.option('--cell-index', 'cell_index',
                                 help='Limit the process to a particular cell (e.g. 14,-11)',
                                 callback=validate_cell_index, default=None)
#: pylint: disable=invalid-name
cell_index_list_option = click.option('--cell-index-list', 'cell_index_list',
                                      help='Limit the process to a file of cells indexes (e.g. 14,-11)',
                                      callback=validate_cell_list, default=None)
#: pylint: disable=invalid-name
year_option = click.option('--year', 'time', help='Limit the process to a particular year',
                           callback=validate_year)


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
                click.echo('Must specify exactly one of --app-config, --load-tasks')
                click.get_current_context().exit(1)

            if app_config is not None:
                config, tasks = load_config(index, app_config, make_config, make_tasks, *args, **kwargs)

            if input_tasks_file:
                config, tasks = load_tasks(input_tasks_file)

            if output_tasks_file:
                num_tasks_saved = save_tasks(config, tasks, output_tasks_file)
                return num_tasks_saved != 0

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


def add_dataset_to_db(index, datasets):
    for dataset in datasets.values:
        index.datasets.add(dataset, with_lineage=False)
        _LOG.info('Dataset added')


def do_nothing(result):
    pass


def _wrap_impl(f, args, kwargs, task):
    """
    Helper method, needs to be at the top level
    """
    return f(task, *args, **kwargs)


def wrap_task(f, *args, **kwargs):
    """
    Turn function `f(task, *args, **kwargs)` into `g(task)` in pickle-able fashion
    """
    return functools.partial(_wrap_impl, f, args, kwargs)


def run_tasks(tasks, executor, run_task, process_result=None, queue_size=50):
    """
    :param tasks: iterable of tasks. Usually a generator to create them as required.
    :param executor: a datacube executor, similar to `distributed.Client` or `concurrent.futures`
    :param run_task: the function used to run a task. Expects a single argument of one of the tasks
    :param process_result: a function to do something based on the result of a completed task. It
                           takes a single argument, the return value from `run_task(task)`
    :param queue_size: How large the queue of tasks should be. Will depend on how fast tasks are
                       processed, and how much memory is available to buffer them.
    """
    click.echo('Starting processing...')
    process_result = process_result or do_nothing
    results = []
    task_queue = itertools.islice(tasks, queue_size)
    for task in task_queue:
        _LOG.info('Running task: %s', task.get('tile_index', str(task)))
        results.append(executor.submit(run_task, task=task))

    click.echo('Task queue filled, waiting for first result...')

    successful = failed = 0
    while results:
        result, results = executor.next_completed(results, None)

        # submit a new _task to replace the one we just finished
        task = next(tasks, None)
        if task:
            _LOG.info('Running task: %s', task.get('tile_index', str(task)))
            results.append(executor.submit(run_task, task=task))

        # Process the result
        try:
            actual_result = executor.result(result)
            process_result(actual_result)
            successful += 1
        except Exception as err:  # pylint: disable=broad-except
            _LOG.exception('Task failed: %s', err)
            failed += 1
            continue
        finally:
            # Release the _task to free memory so there is no leak in executor/scheduler/worker process
            executor.release(result)

    click.echo('%d successful, %d failed' % (successful, failed))
