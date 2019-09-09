import pytest
import mock
import json

from datacube.testutils import write_files

from datacube.utils.aws import (
    _fetch_text,
    ec2_current_region,
    auto_find_region,
    get_aws_settings,
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


def test_get_aws_settings(monkeypatch):

    pp = write_files({
        "config": """
[default]
region = us-west-2

[profile east]
region = us-east-1
[profile no_region]
""",
        "credentials": """
[default]
aws_access_key_id = AKIAWYXYXYXYXYXYXYXY
aws_secret_access_key = fake-fake-fake
[east]
aws_access_key_id = AKIAEYXYXYXYXYXYXYXY
aws_secret_access_key = fake-fake-fake
"""
    })

    assert (pp/"credentials").exists()
    assert (pp/"config").exists()

    for e in ("AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN"
              "AWS_DEFAULT_REGION AWS_DEFAULT_OUTPUT AWS_PROFILE "
              "AWS_ROLE_SESSION_NAME AWS_CA_BUNDLE "
              "AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE").split(" "):
        monkeypatch.delenv(e, raising=False)

    monkeypatch.setenv("AWS_CONFIG_FILE", str(pp/"config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(pp/"credentials"))

    aws, creds = get_aws_settings()
    assert aws['region_name'] == 'us-west-2'
    assert aws['aws_access_key_id'] == 'AKIAWYXYXYXYXYXYXYXY'
    assert aws['aws_secret_access_key'] == 'fake-fake-fake'

    aws, creds = get_aws_settings(profile='east')
    assert aws['region_name'] == 'us-east-1'
    assert aws['aws_access_key_id'] == 'AKIAEYXYXYXYXYXYXYXY'
    assert aws['aws_secret_access_key'] == 'fake-fake-fake'

    aws, creds = get_aws_settings(aws_unsigned=True)
    assert creds is None
    assert aws['region_name'] == 'us-west-2'
    assert aws['aws_unsigned'] is True

    aws, creds = get_aws_settings(profile="no_region",
                                  region_name="us-west-1",
                                  aws_unsigned=True)

    assert aws['region_name'] == 'us-west-1'
    assert aws['aws_unsigned'] is True

    with mock.patch('datacube.utils.aws._fetch_text',
                    return_value=_json(region="mordor")):
        aws, creds = get_aws_settings(profile="no_region",
                                      aws_unsigned=True)

        assert aws['region_name'] == 'mordor'
        assert aws['aws_unsigned'] is True
