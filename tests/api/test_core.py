from datacube.api.query import GroupDatasetsPolicy

from datacube import Datacube
import datetime


def test_grouping_datasets():
    def group_func(d):
        return d['time']
    dimension = 'time'
    units = None
    datasets = [
        {'time': datetime.datetime(2016, 1, 1), 'value': 'foo'},
        {'time': datetime.datetime(2016, 1, 1), 'value': 'flim'},
        {'time': datetime.datetime(2016, 2, 1), 'value': 'bar'}
    ]

    group_by = GroupDatasetsPolicy(dimension, group_func, dict(units=units), sort_key=group_func)
    grouped = Datacube.group_datasets(datasets, group_by)

    assert str(grouped.time.dtype) == 'datetime64[ns]'
    assert grouped.loc['2016-01-01':'2016-01-15']


def test_grouped_datasets_should_be_in_consistent_order():
    datasets = [
        {'time': datetime.datetime(2016, 1, 1, 0, 1), 'value': 'foo'},
        {'time': datetime.datetime(2016, 1, 1, 0, 2), 'value': 'flim'},
        {'time': datetime.datetime(2016, 2, 1, 0, 1), 'value': 'bar'}
    ]

    grouped = _group_datasets_by_date(datasets)

    # Swap the two elements which get grouped together
    datasets[0], datasets[1] = datasets[1], datasets[0]
    grouped_2 = _group_datasets_by_date(datasets)

    assert len(grouped) == len(grouped_2) == 2
    assert all(grouped.values == grouped_2.values)


def _group_datasets_by_date(datasets):
    def group_func(d):
        return d['time'].date()

    def sort_key(d):
        return d['time']
    dimension = 'time'
    units = None

    group_by = GroupDatasetsPolicy(dimension, group_func, dict(units=units), sort_key=sort_key)
    return Datacube.group_datasets(datasets, group_by)
