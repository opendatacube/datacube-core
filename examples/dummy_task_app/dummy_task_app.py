#!/usr/bin/env python
""" Sample task app
"""


import random
import click
from datacube.ui import click as ui
from datacube.ui.task_app import task_app, task_app_options, run_tasks, wrap_task

APP_NAME = 'dummy'


def random_sleep(amount_secs=0.1, prop=0.5):
    """emulate processing time variance"""
    from time import sleep
    from random import uniform

    if uniform(0, 1) < prop:
        sleep(amount_secs)


def unused(*_, **_unused):
    """Used to silence pylint warnings"""
    pass


def make_config(db_index, config, **opts):
    """Called after parsing command line arguments and initialising database index.

    The idea is to inject extra configs based on the content of the database,
    app config file and command line arguments.

    """

    click.echo("------------------------------")
    click.echo(opts)
    click.echo("------------------------------")

    # in real program you might need these
    unused(db_index)

    # Override config value with command line value, or set to default value of 10
    num_tasks = opts.get('num_tasks')
    if num_tasks is None:
        num_tasks = config.get('num_tasks', 10)
    config['num_tasks'] = num_tasks

    return config


def make_tasks(db_index, config, **opts):
    """ Generate a task list.

    This function receives config created by `make_config` as well as database index
    """
    num_tasks = config['num_tasks']

    unused(db_index, opts)

    for i in range(num_tasks):
        print('Generating task: {}'.format(i))
        yield {'val': i}


def run_task(task, op):
    """ Runs across multiple cpus/nodes
    """
    from math import sqrt

    ops = {'sqrt': sqrt,
           'pow2': lambda x: x*x}

    random_sleep(1, 0.1)  # Sleep for 1 second 10% of the time

    val = task['val']

    if val == 666:
        click.echo('Injecting failure')
        raise IOError('Fake IO Error')

    result = ops[op](val)
    click.echo('{} => {}'.format(val, result))

    return result


@click.command(name=APP_NAME)
@ui.pass_index(app_name=APP_NAME)
@task_app_options
@click.option('--num-tasks', type=int, help='Sample argument: number of tasks to generate')
@task_app(make_config=make_config, make_tasks=make_tasks)
def app_main(db_index, config, tasks, executor, **opts):
    """
    make_config => config
    config => make_tasks => tasks
    """
    from pickle import dumps

    unused(db_index, opts, config)

    click.echo('Using executor {}'.format(repr(executor)))
    task_runner = wrap_task(run_task, config['op'])

    click.echo('Task function size: {}'.format(
        len(dumps(task_runner))
    ))

    run_tasks(tasks, executor, task_runner, queue_size=10)

    return 0


if __name__ == '__main__':
    random.seed()
    app_main()
