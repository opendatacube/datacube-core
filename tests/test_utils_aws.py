import pytest
import mock
import json
from botocore.credentials import ReadOnlyCredentials

from datacube.testutils import write_files
from datacube.utils.aws import (
    _fetch_text,
    ec2_current_region,
    auto_find_region,
    get_aws_settings,
    mk_boto_session,
    get_creds_with_retry,
    s3_url_parse,
    s3_fmt_range,
    s3_client,
    s3_dump,
    s3_fetch,
    _s3_cache_key,
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


def test_get_aws_settings(monkeypatch, without_aws_env):

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

    monkeypatch.setenv("AWS_CONFIG_FILE", str(pp/"config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(pp/"credentials"))

    aws, creds = get_aws_settings()
    assert aws['region_name'] == 'us-west-2'
    assert aws['aws_access_key_id'] == 'AKIAWYXYXYXYXYXYXYXY'
    assert aws['aws_secret_access_key'] == 'fake-fake-fake'

    sess = mk_boto_session(profile="no_region",
                           creds=creds.get_frozen_credentials(),
                           region_name="mordor")

    assert sess.get_credentials().get_frozen_credentials() == creds.get_frozen_credentials()

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

    assert creds is None
    assert aws['region_name'] == 'us-west-1'
    assert aws['aws_unsigned'] is True

    with mock.patch('datacube.utils.aws._fetch_text',
                    return_value=_json(region="mordor")):
        aws, creds = get_aws_settings(profile="no_region",
                                      aws_unsigned=True)

        assert aws['region_name'] == 'mordor'
        assert aws['aws_unsigned'] is True


@mock.patch('datacube.utils.aws.get_creds_with_retry', return_value=None)
def test_get_aws_settings_no_credentials(without_aws_env):
    # get_aws_settings should fail when credentials are not available
    with pytest.raises(ValueError, match="Couldn't get credentials"):
        aws, creds = get_aws_settings(region_name="fake")


def test_creds_with_retry():
    session = mock.MagicMock()
    session.get_credentials = mock.MagicMock(return_value=None)

    assert get_creds_with_retry(session, 2, 0.01) is None
    assert session.get_credentials.call_count == 2


def test_s3_basics(without_aws_env):
    from numpy import s_
    from botocore.credentials import ReadOnlyCredentials

    assert s3_url_parse('s3://bucket/key') == ('bucket', 'key')
    assert s3_url_parse('s3://bucket/key/') == ('bucket', 'key/')
    assert s3_url_parse('s3://bucket/k/k/key') == ('bucket', 'k/k/key')

    with pytest.raises(ValueError):
        s3_url_parse("file://some/path")

    assert s3_fmt_range((0, 3)) == "bytes=0-2"
    assert s3_fmt_range(s_[4:10]) == "bytes=4-9"
    assert s3_fmt_range(s_[:10]) == "bytes=0-9"
    assert s3_fmt_range(None) is None

    for bad in (s_[10:], s_[-2:3], s_[:-3], (-1, 3), (3, -1), s_[1:100:3]):
        with pytest.raises(ValueError):
            s3_fmt_range(bad)

    creds = ReadOnlyCredentials('fake-key', 'fake-secret', None)

    assert str(s3_client(region_name='kk')._endpoint) == 's3(https://s3.kk.amazonaws.com)'
    assert str(s3_client(region_name='kk', use_ssl=False)._endpoint) == 's3(http://s3.kk.amazonaws.com)'

    s3 = s3_client(region_name='us-west-2', creds=creds)
    assert s3 is not None


def test_s3_io(monkeypatch, without_aws_env):
    import moto
    from numpy import s_

    url = "s3://bucket/file.txt"
    bucket, _ = s3_url_parse(url)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")

    with moto.mock_s3():
        s3 = s3_client(region_name='kk')
        s3.create_bucket(Bucket=bucket)
        assert s3_dump(b"33", url, s3=s3) is True
        assert s3_fetch(url, s3=s3) == b"33"

        assert s3_dump(b"0123456789ABCDEF", url, s3=s3) is True
        assert s3_fetch(url, range=s_[:4], s3=s3) == b"0123"
        assert s3_fetch(url, range=s_[3:8], s3=s3) == b"34567"

        with pytest.raises(ValueError):
            s3_fetch(url, range=s_[::2], s3=s3)


@mock.patch('datacube.utils.aws.ec2_current_region', return_value="us-west-2")
def test_s3_client_cache(monkeypatch, without_aws_env):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")

    s3 = s3_client(cache=True)
    assert s3 is s3_client(cache=True)
    assert s3 is s3_client(cache='purge')
    assert s3_client(cache='purge') is None
    assert s3 is not s3_client(cache=True)

    opts = (dict(),
            dict(region_name="foo"),
            dict(region_name="bar"),
            dict(profile="foo"),
            dict(profile="foo", region_name="xxx"),
            dict(profile="bar"),
            dict(creds=ReadOnlyCredentials('fake1', '...', None)),
            dict(creds=ReadOnlyCredentials('fake1', '...', None), region_name='custom'),
            dict(creds=ReadOnlyCredentials('fake2', '...', None)))

    keys = set(_s3_cache_key(**o) for o in opts)
    assert len(keys) == len(opts)
