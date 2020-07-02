# coding=utf-8
"""
Module
"""

from datacube.ui.task_app import task_app, run_tasks, wrap_task
import datacube.executor


def make_test_config(index, config, **kwargs):
    assert index == 'Fake Index'
    assert 'config_arg' in kwargs

    config['some_item'] = 'make_test_config'
    config['num_tasks'] = config.get('num_tasks', 3)
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


def test_task_app_with_no_tasks(tmpdir):
    index = 'Fake Index'

    app_config = tmpdir.join("app_config.yaml")
    app_config.write('name: Test Config\r\n'
                     'description: This is my test app config file\r\n'
                     'num_tasks: 0')

    taskfile = tmpdir.join("tasks.bin")
    assert not taskfile.check()

    my_test_app(index, app_config=str(app_config), output_tasks_file=str(taskfile), config_arg=True, task_arg=True)

    assert not taskfile.check()


def test_task_app_year_splitting():
    import pandas as pd
    from datacube.ui.task_app import validate_year, break_query_into_years
    one_millisecond = pd.Timedelta('1 ms')

    def is_close(ts1, ts2, max_delta=one_millisecond):
        return abs(pd.Timestamp(ts1) - pd.Timestamp(ts2)) < max_delta

    assert validate_year(None, None, None) is None
    year_range = validate_year(None, None, '1996-2004')
    assert is_close(year_range[0], '1996-01-01 00:00:00')
    assert is_close(year_range[1], '2004-12-31 23:59:59.999999999')

    year_range = validate_year(None, None, '2003')
    assert is_close(year_range[0], '2003-01-01 00:00:00')
    assert is_close(year_range[1], '2003-12-31 23:59:59.999999999')

    # Test that a no year range makes a single query
    year_range = None
    query = break_query_into_years(year_range)
    assert len(query) == 1
    assert query[0] == {}

    # Test that a no year range makes a single query with additional params
    year_range = None
    test_cell_index = (11, 12)
    query = break_query_into_years(year_range, cell_index=test_cell_index)
    assert len(query) == 1
    assert query[0] == {'cell_index': test_cell_index}

    # Test that a single year makes a single query
    year_range = ("1996", "1996")
    query = break_query_into_years(year_range)
    assert len(query) == 1
    assert is_close(query[0]['time'][0], '1996-01-01 00:00:00')
    assert is_close(query[0]['time'][1], '1996-12-31 23:59:59.999999999')

    # Test that a multiple years makes multiple queries
    year_range = ("1996", "1997")
    query = break_query_into_years(year_range)
    assert len(query) == 2
    assert is_close(query[0]['time'][0], '1996-01-01 00:00:00')
    assert is_close(query[0]['time'][1], '1996-12-31 23:59:59.999999999')
    assert is_close(query[1]['time'][0], '1997-01-01 00:00:00')
    assert is_close(query[1]['time'][1], '1997-12-31 23:59:59.999999999')

    # Check that additional kwargs can be used in the query
    year_range = ("1996", "1997")
    test_cell_index = (11, 12)
    query = break_query_into_years(year_range, cell_index=test_cell_index)
    assert len(query) == 2
    assert is_close(query[0]['time'][0], '1996-01-01 00:00:00')
    assert is_close(query[0]['time'][1], '1996-12-31 23:59:59.999999999')
    assert query[0]['cell_index'] == test_cell_index
    assert is_close(query[1]['time'][0], '1997-01-01 00:00:00')
    assert is_close(query[1]['time'][1], '1997-12-31 23:59:59.999999999')
    assert query[1]['cell_index'] == test_cell_index


def test_task_app_cell_index(tmpdir):
    from datacube.ui.task_app import validate_cell_index, validate_cell_list, cell_list_to_file

    assert validate_cell_index(None, None, None) is None
    assert validate_cell_index(None, None, '17,-12') == (17, -12)

    cell_list = [(17, 12), (16, -10), (-23, 0)]

    cell_list_file = tmpdir.join('cell_list.txt')
    if cell_list_file.exists():
        cell_list_file.unlink()
    assert not cell_list_file.check()

    cell_list_to_file(str(cell_list_file), cell_list)

    assert cell_list_file.check()

    assert validate_cell_list(None, None, None) is None
    assert validate_cell_list(None, None, str(cell_list_file)) == cell_list


def test_run_tasks():
    executor = datacube.executor.SerialExecutor()
    tasks = ({'val': i} for i in range(3))
    tasks_to_do = list(range(3))

    def task_func(task):
        x = task['val']
        return (x, x**2)

    def process_result_func(result):
        assert result[0]**2 == result[1]
        tasks_to_do.remove(result[0])

    run_tasks(tasks, executor, task_func, process_result_func)
    assert not tasks_to_do

    # no task proc specified
    tasks = ({'val': i} for i in range(3))
    run_tasks(tasks, executor, task_func)


def test_wrap_task():
    def task_with_args(task, a, b):
        return (task, a, b)

    assert task_with_args(1, 2, 'a') == (1, 2, 'a')
    assert wrap_task(task_with_args, 'a', 'b')(0) == (0, 'a', 'b')
