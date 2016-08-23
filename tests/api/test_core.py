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

    grouped = Datacube.product_sources(datasets, group_func, dimension, units)

    assert str(grouped.time.dtype) == 'datetime64[ns]'
    assert grouped.loc['2016-01-01':'2016-01-15']
