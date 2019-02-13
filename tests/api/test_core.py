from uuid import UUID
from types import SimpleNamespace
from datacube.api.query import GroupBy

from datacube import Datacube
import datetime


def test_grouping_datasets():
    def group_func(d):
        return d.time
    dimension = 'time'
    units = None
    datasets = [
        SimpleNamespace(time=datetime.datetime(2016, 1, 1), value='foo', id=UUID(int=10)),
        SimpleNamespace(time=datetime.datetime(2016, 2, 1), value='bar', id=UUID(int=1)),
        SimpleNamespace(time=datetime.datetime(2016, 1, 1), value='flim', id=UUID(int=9)),
    ]

    group_by = GroupBy(dimension, group_func, units, sort_key=group_func)
    grouped = Datacube.group_datasets(datasets, group_by)
    dss = grouped.isel(time=0).values[()]
    assert isinstance(dss, tuple)
    assert len(dss) == 2
    assert [ds.value for ds in dss] == ['flim', 'foo']

    dss = grouped.isel(time=1).values[()]
    assert isinstance(dss, tuple)
    assert len(dss) == 1
    assert [ds.value for ds in dss] == ['bar']

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

    group_by = GroupBy(dimension, group_func, units, sort_key)
    return Datacube.group_datasets(datasets, group_by)
