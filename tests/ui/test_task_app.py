# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.ui.task_app import task_app


def make_test_config(index, config, **kwargs):
    assert index == 'Fake Index'
    assert 'config_arg' in kwargs

    config['some_item'] = 'make_test_config'
    config['num_tasks'] = 3
    return config


def make_test_tasks(index, config, **kwargs):
    assert index == 'Fake Index'
    assert 'task_arg' in kwargs

    num_tasks = config['num_tasks']
    for i in range(0, num_tasks):
        yield 'Task: {}'.format(i)


@task_app(make_config=make_test_config, make_tasks=make_test_tasks)
def my_test_app(index, config, tasks, **kwargs):
    assert index == 'Fake Index'
    assert config['some_item'] == 'make_test_config'
    assert 'app_arg' in kwargs

    task_list = list(tasks)
    assert len(task_list) == config['num_tasks']


def test_task_app(tmpdir):
    index = 'Fake Index'

    app_config = tmpdir.join("app_config.yaml")
    app_config.write('name: Test Config\r\n'
                     'description: This is my test app config file')

    my_test_app(index, str(app_config), app_arg=True, config_arg=True, task_arg=True)


def test_task_app_with_task_file(tmpdir):
    index = 'Fake Index'

    app_config = tmpdir.join("app_config.yaml")
    app_config.write('name: Test Config\r\n'
                     'description: This is my test app config file')

    taskfile = tmpdir.join("tasks.bin")
    assert not taskfile.check()

    my_test_app(index, app_config=str(app_config), output_tasks_file=str(taskfile), config_arg=True, task_arg=True)

    assert taskfile.check()

    my_test_app(index, input_tasks_file=str(taskfile), app_arg=True)
