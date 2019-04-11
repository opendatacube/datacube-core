import pytest
import os

from datacube.utils.rio import (
    activate_rio_env,
    deactivate_rio_env,
    get_rio_env,
)


def test_rio_env_no_aws():
    # make sure we start without env configured
    assert get_rio_env() == {}

    ee = activate_rio_env()
    assert isinstance(ee, dict)
    assert ee == get_rio_env()
    assert 'GDAL_DISABLE_READDIR_ON_OPEN' not in ee
    assert 'GDAL_DATA' in ee
    assert 'AWS_ACCESS_KEY_ID' not in ee

    ee = activate_rio_env(cloud_defaults=True)
    assert 'GDAL_DISABLE_READDIR_ON_OPEN' in ee
    assert ee == get_rio_env()

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws():
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


@pytest.mark.skipif(os.environ.get('TRAVIS', None) == 'true',
                    reason='Not running auto_region tests on Travis')
def test_rio_aws_auto_region():
    ee = activate_rio_env(aws={})
    assert 'AWS_REGION' in ee
