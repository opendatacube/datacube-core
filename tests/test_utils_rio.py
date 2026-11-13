# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import mock

import moto
import pytest
import rasterio.env

from datacube.testutils import write_files
from datacube.utils.aws import configure_s3_access
from datacube.utils.rio import (
    GDALManagedAWSSession,
    activate_from_config,
    activate_rio_env,
    deactivate_rio_env,
    get_rio_env,
    set_default_rio_config,
)
from datacube.utils.rio._rio import SECRET_KEYS, _state


def test_rio_env_no_aws() -> None:
    deactivate_rio_env()

    # make sure we start without env configured
    assert get_rio_env() == {}

    ee = activate_rio_env(FAKE_OPTION=1)
    assert ee == get_rio_env()
    assert "GDAL_DISABLE_READDIR_ON_OPEN" not in ee
    if os.getenv("GDAL_DATA", None):
        assert "GDAL_DATA" in ee
    assert ee.get("FAKE_OPTION") == 1
    assert "AWS_ACCESS_KEY_ID" not in ee

    ee = activate_rio_env(cloud_defaults=True)
    assert "GDAL_DISABLE_READDIR_ON_OPEN" in ee
    assert ee == get_rio_env()

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws() -> None:
    deactivate_rio_env()

    # make sure we start without env configured
    assert get_rio_env() == {}

    with pytest.raises(ValueError):
        activate_rio_env(aws="something")  # type: ignore[arg-type]

    # note: setting region_name to avoid auto-lookup
    ee = activate_rio_env(aws={"aws_unsigned": True, "region_name": "us-west-1"})

    assert ee.get("AWS_NO_SIGN_REQUEST") == "YES"

    ee = activate_rio_env(
        cloud_defaults=True,
        aws={
            "aws_secret_access_key": "blabla",
            "aws_access_key_id": "not a real one",
            "aws_session_token": "faketoo",
            "region_name": "us-west-1",
        },
    )

    assert "AWS_NO_SIGN_REQUEST" not in ee
    # check secrets are sanitized
    assert ee.get("AWS_ACCESS_KEY_ID") == "xx..xx"
    assert ee.get("AWS_SECRET_ACCESS_KEY") == "xx..xx"
    assert ee.get("AWS_SESSION_TOKEN") == "xx..xx"

    assert ee.get("AWS_REGION") == "us-west-1"

    # check sanitize can be turned off
    ee = get_rio_env(sanitize=False)
    assert ee.get("AWS_SECRET_ACCESS_KEY") == "blabla"
    assert ee.get("AWS_ACCESS_KEY_ID") == "not a real one"
    assert ee.get("AWS_SESSION_TOKEN") == "faketoo"

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws_auto_region(monkeypatch, without_aws_env) -> None:
    import datacube.utils.aws

    pp = write_files(
        {
            "config": """[default]
"""
        }
    )

    assert (pp / "config").exists()
    monkeypatch.setenv("AWS_CONFIG_FILE", str(pp / "config"))

    assert datacube.utils.aws.botocore_default_region() is None

    aws = {
        "aws_secret_access_key": "blabla",
        "aws_access_key_id": "not a real one",
        "aws_session_token": "faketoo",
    }

    with mock.patch("datacube.utils.aws.ec2_current_region", return_value="TT"):
        ee = activate_rio_env(aws=aws)
        assert ee.get("AWS_REGION") == "TT"

    with mock.patch("datacube.utils.aws.ec2_current_region", return_value=None):
        ee = activate_rio_env(aws=aws)
        assert "AWS_REGION" not in ee

        with pytest.raises(ValueError):
            activate_rio_env(aws={"region_name": "auto"})

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_env_aws_auto_region_dummy() -> None:
    """Just call it we don't know if it will succeed"""

    with moto.mock_aws():
        # at least it should not raise error since we haven't asked for region_name='auto'
        _ = activate_rio_env(aws={})

        deactivate_rio_env()
        assert get_rio_env() == {}


def test_rio_env_via_config() -> None:
    ee = activate_from_config()
    assert ee is not None

    # Second call should not change anything
    assert activate_from_config() is None

    set_default_rio_config(aws=None, cloud_defaults=True)

    # config change should activate new env
    ee = activate_from_config()
    assert ee is not None
    assert "GDAL_DISABLE_READDIR_ON_OPEN" in ee

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_configure_aws_access(monkeypatch, without_aws_env, dask_client) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "fake-region")

    creds = configure_s3_access()
    assert creds is not None
    cc = creds.get_frozen_credentials()
    assert cc.access_key == "fake-key-id"
    assert cc.secret_key == "fake-secret"
    assert cc.token is None

    ee = activate_from_config()
    assert ee is not None
    assert "AWS_ACCESS_KEY_ID" in ee
    assert "AWS_SECRET_ACCESS_KEY" in ee
    assert "AWS_REGION" in ee
    assert "AWS_SESSION_TOKEN" not in ee

    ee = get_rio_env(sanitize=False)
    assert ee is not None
    assert ee["AWS_ACCESS_KEY_ID"] == "fake-key-id"
    assert ee["AWS_SECRET_ACCESS_KEY"] == "fake-secret"
    assert ee["AWS_REGION"] == "fake-region"
    assert ee["GDAL_DISABLE_READDIR_ON_OPEN"] == "EMPTY_DIR"

    ee_local = ee
    client = dask_client

    creds = configure_s3_access(client=client)
    assert creds is not None
    cc = creds.get_frozen_credentials()
    assert cc.access_key == "fake-key-id"
    assert cc.secret_key == "fake-secret"
    assert cc.token is None

    ee = client.submit(activate_from_config).result()
    assert ee is not None
    assert "AWS_ACCESS_KEY_ID" in ee
    assert "AWS_SECRET_ACCESS_KEY" in ee
    assert "AWS_REGION" in ee
    assert "AWS_SESSION_TOKEN" not in ee

    def _activate_and_get(sanitize: bool = True):
        activate_from_config()
        return get_rio_env(sanitize=sanitize)

    ee = client.submit(_activate_and_get, sanitize=False).result()
    assert ee == ee_local


def test_rio_env_aws_gdal_credentials(monkeypatch, without_aws_env) -> None:
    deactivate_rio_env()
    # static keys in the process environment must not be copied into GDAL config
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")

    with mock.patch("boto3.Session", side_effect=AssertionError("boto3 used")):
        ee = activate_rio_env(
            aws={
                "gdal_credentials": True,
                "region_name": "us-west-1",
                "profile_name": "odc",
                "requester_pays": True,
            }
        )

    assert isinstance(_state().env.session, GDALManagedAWSSession)
    ee = get_rio_env(sanitize=False)
    assert not set(SECRET_KEYS) & set(ee)
    assert ee["AWS_NO_SIGN_REQUEST"] == "NO"
    assert ee["AWS_REGION"] == "us-west-1"
    assert ee["AWS_PROFILE"] == "odc"
    assert ee["AWS_REQUEST_PAYER"] == "requester"

    # rasterio.open(s3://..) must not stack a boto3 AWSSession on top
    @rasterio.env.ensure_env_with_credentials
    def _open(fp):
        return rasterio.env.getenv()

    assert not set(SECRET_KEYS) & set(_open("s3://bucket/file.tif"))

    deactivate_rio_env()
    assert get_rio_env() == {}


def test_rio_configure_aws_access_web_identity(monkeypatch, without_aws_env) -> None:
    pp = write_files({"token": "fake-oidc-token"})
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(pp / "token"))
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/odc-reader")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(pp / "missing"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(pp / "missing"))

    with moto.mock_aws(), mock.patch.dict(os.environ):
        # moto sets fake static keys, remove them so web identity is used
        for k in SECRET_KEYS:
            os.environ.pop(k, None)

        creds = configure_s3_access(gdal_credentials=True)
        assert creds is not None
        assert creds.method == "assume-role-with-web-identity"

        ee = activate_from_config()
        assert ee is not None
        assert isinstance(_state().env.session, GDALManagedAWSSession)

        ee = get_rio_env(sanitize=False)
        assert not set(SECRET_KEYS) & set(ee)
        assert ee["AWS_NO_SIGN_REQUEST"] == "NO"
        assert ee["AWS_REGION"] == "ap-southeast-2"

    with pytest.raises(ValueError):
        configure_s3_access(gdal_credentials=True, driver="rio")

    set_default_rio_config()
    deactivate_rio_env()
