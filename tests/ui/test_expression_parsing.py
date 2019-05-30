from datacube.ui import parse_expressions
from datacube.model import Range
from datetime import datetime


def test_between_expression():
    q = parse_expressions('time in [2014, 2015]')
    assert 'time' in q
    r = q['time']
    assert isinstance(r, Range)
    assert isinstance(r.begin, datetime)
    assert isinstance(r.end, datetime)

    for k in ('lon', 'lat', 'x', 'y'):
        q = parse_expressions('{} in [10, 11.3]'.format(k))
        assert k in q
        r = q[k]
        assert isinstance(r, Range)
        assert isinstance(r.begin, float)
        assert isinstance(r.end, float)
        assert r == Range(10, 11.3)
