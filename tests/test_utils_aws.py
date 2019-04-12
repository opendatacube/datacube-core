import pytest
import mock
import json

from datacube.utils.aws import (
    _fetch_text,
    ec2_current_region,
    auto_find_region,
)


def _json(**kw):
    return json.dumps(kw)


def mock_urlopen(text, code=200):
    m = mock.MagicMock()
    m.getcode.return_value = code
    m.read.return_value = text.encode('utf8')
    m.__enter__.return_value = m
    return m


def test_ec2_current_region():
    tests = [(None, None),
             (_json(region='TT'), 'TT'),
             (_json(x=3), None),
             ('not valid json', None)]

    for (rv, expect) in tests:
        with mock.patch('datacube.utils.aws._fetch_text', return_value=rv):
            assert ec2_current_region() == expect


@mock.patch('datacube.utils.aws.botocore_default_region',
            return_value=None)
def test_auto_find_region(*mocks):
    with mock.patch('datacube.utils.aws._fetch_text', return_value=None):
        with pytest.raises(ValueError):
            auto_find_region()

    with mock.patch('datacube.utils.aws._fetch_text', return_value=_json(region='TT')):
        assert auto_find_region() == 'TT'


@mock.patch('datacube.utils.aws.botocore_default_region',
            return_value='tt-from-botocore')
def test_auto_find_region_2(*mocks):
    assert auto_find_region() == 'tt-from-botocore'


def test_fetch_text():
    with mock.patch('datacube.utils.aws.urlopen',
                    return_value=mock_urlopen('', 505)):
        assert _fetch_text('http://localhost:8817') is None

    with mock.patch('datacube.utils.aws.urlopen',
                    return_value=mock_urlopen('text', 200)):
        assert _fetch_text('http://localhost:8817') == 'text'

    def fake_urlopen(*args, **kw):
        raise IOError("Always broken")

    with mock.patch('datacube.utils.aws.urlopen', fake_urlopen):
        assert _fetch_text('http://localhost:8817') is None
