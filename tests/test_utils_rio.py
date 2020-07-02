import pytest
import mock
import os

from datacube.testutils import write_files
from datacube.utils.rio import (
    activate_rio_env,
    deactivate_rio_env,
    get_rio_env,
    set_default_rio_config,
    activate_from_config,
    configure_s3_access,
)


def test_rio_env_no_aws():
    deactivate_rio_env()

    # make sure we start without env configured
    assert get_rio_env() == {}

    ee = activate_rio_env(FAKE_OPTION=1)
    assert isinstance(ee, dict)
    assert ee == get_rio_env()
    assert 'GDAL_DISABLE_READDIR_ON_OPEN' not in ee
    if os.getenv('GDAL_DATA', None):
        assert 'GDAL_DATA' in ee
    assert ee.get('FAKE_OPTION') == 1
    assert 'AWS_ACCESS_KEY_ID' not in ee

    ee = activate_rio_env(cloud_defaults=True)
    assert 'GDAL_DISABLE_READDIR_ON_OPEN' in ee
    assert ee == get_rio_env()

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws():
    deactivate_rio_env()

    # make sure we start without env configured
    assert get_rio_env() == {}

    with pytest.raises(ValueError):
        activate_rio_env(aws='something')

    # note: setting region_name to avoid auto-lookup
    ee = activate_rio_env(aws=dict(aws_unsigned=True,
                                   region_name='us-west-1'))

    assert ee.get('AWS_NO_SIGN_REQUEST') == 'YES'

    ee = activate_rio_env(cloud_defaults=True,
                          aws=dict(aws_secret_access_key='blabla',
                                   aws_access_key_id='not a real one',
                                   aws_session_token='faketoo',
                                   region_name='us-west-1'))

    assert 'AWS_NO_SIGN_REQUEST' not in ee
    # check secrets are sanitized
    assert ee.get('AWS_ACCESS_KEY_ID') == 'xx..xx'
    assert ee.get('AWS_SECRET_ACCESS_KEY') == 'xx..xx'
    assert ee.get('AWS_SESSION_TOKEN') == 'xx..xx'

    assert ee.get('AWS_REGION') == 'us-west-1'

    # check sanitize can be turned off
    ee = get_rio_env(sanitize=False)
    assert ee.get('AWS_SECRET_ACCESS_KEY') == 'blabla'
    assert ee.get('AWS_ACCESS_KEY_ID') == 'not a real one'
    assert ee.get('AWS_SESSION_TOKEN') == 'faketoo'

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws_auto_region(monkeypatch, without_aws_env):
    import datacube.utils.aws

    pp = write_files({
        "config": """[default]
"""})

    assert (pp/"config").exists()
    monkeypatch.setenv("AWS_CONFIG_FILE", str(pp/"config"))

    assert datacube.utils.aws.botocore_default_region() is None

    aws = dict(aws_secret_access_key='blabla',
               aws_access_key_id='not a real one',
               aws_session_token='faketoo')

    with mock.patch('datacube.utils.aws.ec2_current_region',
                    return_value='TT'):
        ee = activate_rio_env(aws=aws)
        assert ee.get('AWS_REGION') == 'TT'

    with mock.patch('datacube.utils.aws.ec2_current_region',
                    return_value=None):
        ee = activate_rio_env(aws=aws)
        assert 'AWS_REGION' not in ee

        with pytest.raises(ValueError):
            activate_rio_env(aws=dict(region_name='auto'))

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws_auto_region_dummy():
    "Just call it we don't know if it will succeed"

    # at least it should not raise error since we haven't asked for region_name='auto'
    ee = activate_rio_env(aws={})
    assert isinstance(ee, dict)

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_via_config():
    ee = activate_from_config()
    assert ee is not None

    # Second call should not change anything
    assert activate_from_config() is None

    set_default_rio_config(aws=None, cloud_defaults=True)

    # config change should activate new env
    ee = activate_from_config()
    assert ee is not None
    assert 'GDAL_DISABLE_READDIR_ON_OPEN' in ee

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_configure_aws_access(monkeypatch, without_aws_env, dask_client):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "fake-region")

    creds = configure_s3_access()
    cc = creds.get_frozen_credentials()
    assert cc.access_key == 'fake-key-id'
    assert cc.secret_key == 'fake-secret'
    assert cc.token is None

    ee = activate_from_config()
    assert ee is not None
    assert 'AWS_ACCESS_KEY_ID' in ee
    assert 'AWS_SECRET_ACCESS_KEY' in ee
    assert 'AWS_REGION' in ee
    assert 'AWS_SESSION_TOKEN' not in ee

    ee = get_rio_env(sanitize=False)
    assert ee is not None
    assert ee['AWS_ACCESS_KEY_ID'] == 'fake-key-id'
    assert ee['AWS_SECRET_ACCESS_KEY'] == 'fake-secret'
    assert ee['AWS_REGION'] == 'fake-region'
    assert ee['GDAL_DISABLE_READDIR_ON_OPEN'] == 'EMPTY_DIR'

    ee_local = ee
    client = dask_client

    creds = configure_s3_access(client=client)
    cc = creds.get_frozen_credentials()
    assert cc.access_key == 'fake-key-id'
    assert cc.secret_key == 'fake-secret'
    assert cc.token is None

    ee = client.submit(activate_from_config).result()
    assert ee is not None
    assert 'AWS_ACCESS_KEY_ID' in ee
    assert 'AWS_SECRET_ACCESS_KEY' in ee
    assert 'AWS_REGION' in ee
    assert 'AWS_SESSION_TOKEN' not in ee

    ee = client.submit(get_rio_env, sanitize=False).result()
    assert ee == ee_local
