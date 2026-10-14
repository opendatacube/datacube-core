# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""rasterio environment management tools"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Literal

import rasterio
import rasterio.env
from deprecat import deprecat
from rasterio.session import AWSSession, DummySession, Session

from datacube.migration import ODC2DeprecationWarning
from datacube.utils.generic import thread_local_cache

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Mapping

    from botocore.credentials import Credentials

_CFG_LOCK = threading.Lock()
_CFG = SimpleNamespace(aws=None, cloud_defaults=False, kwargs={}, epoch=0)


SECRET_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def _sanitize(opts: Mapping, keys) -> dict:
    return {k: (v if k not in keys else "xx..xx") for k, v in opts.items()}


class GDALManagedAWSSession(Session):
    """AWS session that leaves obtaining and refreshing credentials to GDAL.

    :class:`rasterio.session.AWSSession` resolves credentials with boto3 and
    passes a frozen copy of them to GDAL, so temporary (STS) credentials stop
    working once they expire. This session only passes on non-secret settings,
    GDAL then finds credentials itself (``AWS_WEB_IDENTITY_TOKEN_FILE`` and
    ``AWS_ROLE_ARN``, ``~/.aws`` files, EC2/ECS metadata, ...) and refreshes
    them as needed.
    """

    def __init__(
        self,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        requester_pays: bool = False,
        aws_unsigned: bool = False,
    ) -> None:
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self.requester_pays = requester_pays
        self.unsigned = aws_unsigned

    @classmethod
    def hascreds(cls, config) -> bool:
        return AWSSession.hascreds(config)

    def get_credential_options(self) -> dict:
        # "NO" is GDAL's default, but setting it explicitly makes rasterio treat
        # this environment as credentialized, otherwise it would push a boto3
        # AWSSession with frozen credentials on top of it when opening s3:// paths.
        opts = {"AWS_NO_SIGN_REQUEST": "YES" if self.unsigned else "NO"}
        if self.region_name:
            opts["AWS_REGION"] = self.region_name
        if self.profile_name:
            opts["AWS_PROFILE"] = self.profile_name
        if self.requester_pays:
            opts["AWS_REQUEST_PAYER"] = "requester"
        if self.endpoint_url:
            opts["AWS_S3_ENDPOINT"] = self.endpoint_url
        return opts


def _state(purge: bool = False):
    """
    .env   None| rasterio.Env
    .epoch -1  | +Int
    """
    return thread_local_cache(
        "__rio_state__", SimpleNamespace(env=None, epoch=-1), purge=purge
    )


def get_rio_env(sanitize: bool = True) -> dict:
    """Get GDAL params configured by rasterio for the current thread.

    :param sanitize: If True replace sensitive Values with 'x'
    """
    env = rasterio.env.local._env  # pylint: disable=protected-access
    if env is None:
        return {}
    opts = env.get_config_options()
    if sanitize:
        opts = _sanitize(opts, SECRET_KEYS)

    return opts


def deactivate_rio_env() -> None:
    """Exit previously configured environment, or do nothing if one wasn't configured."""
    state = _state(purge=True)

    if state.env is not None:
        state.env.__exit__(None, None, None)


def activate_rio_env(
    aws: Literal["auto"] | dict | None = None, cloud_defaults: bool = False, **kwargs
) -> dict:
    """Inject activated rasterio.Env into current thread.

    This de-activates previously setup environment.

    :param aws: Dictionary of options for rasterio.session.AWSSession
                OR 'auto' -- session = rasterio.session.AWSSession()

                With ``gdal_credentials=True`` in the dictionary
                :class:`GDALManagedAWSSession` is used instead, and GDAL
                obtains and refreshes credentials itself.

    :param cloud_defaults: When True inject settings for reading COGs
    :param kwargs: Passed on to rasterio.Env(..) constructor
    """
    session: Session = DummySession()

    if aws is not None:
        if not (aws == "auto" or isinstance(aws, dict)):
            raise ValueError('Only support: None|"auto"|{..} for `aws` parameter')

        aws = {} if aws == "auto" else dict(**aws)
        gdal_credentials = aws.pop("gdal_credentials", False)
        region_name = aws.get("region_name", "auto")

        if region_name == "auto":
            from datacube.utils.aws import auto_find_region

            try:
                aws["region_name"] = auto_find_region()
            except ValueError as e:
                # only treat it as error if it was requested by user
                if "region_name" in aws:
                    raise e

        session_cls = GDALManagedAWSSession if gdal_credentials else AWSSession
        session = session_cls(**aws)

    opts = (
        {
            "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
            "GDAL_HTTP_MAX_RETRY": "10",
            "GDAL_HTTP_RETRY_DELAY": "0.5",
        }
        if cloud_defaults
        else {}
    )

    opts.update(**kwargs)

    state = _state()

    if state.env is not None:
        state.env.__exit__(None, None, None)

    env = rasterio.Env(session=session, **opts)
    env.__enter__()
    state.env = env
    state.epoch = -1

    return get_rio_env()


def activate_from_config() -> dict | None:
    """Check if this threads needs to reconfigure, then does reconfigure.

    - Does nothing if this thread is already configured and configuration hasn't changed.
    - Configures current thread with default rio settings
    """
    cfg = _CFG
    state = _state()

    if cfg.epoch != state.epoch:
        ee = activate_rio_env(
            aws=cfg.aws, cloud_defaults=cfg.cloud_defaults, **cfg.kwargs
        )
        state.epoch = cfg.epoch
        return ee

    return None


def set_default_rio_config(aws=None, cloud_defaults: bool = False, **kwargs) -> None:
    """Setup default configuration for rasterio/GDAL.

    Doesn't actually activate one, just stores configuration for future
    use from IO threads.

    :param aws: Dictionary of options for rasterio.session.AWSSession
                OR 'auto' -- session = rasterio.session.AWSSession()

    :param cloud_defaults: When True inject settings for reading COGs
    :param kwargs: Passed on to rasterio.Env(..) constructor
    """
    global _CFG  # pylint: disable=global-statement

    with _CFG_LOCK:
        _CFG = SimpleNamespace(
            aws=aws, cloud_defaults=cloud_defaults, kwargs=kwargs, epoch=_CFG.epoch + 1
        )


@deprecat(
    reason="This method is deprecated. Use datacube.utils.aws.configure_s3_access instead.",
    version="1.9.7",
    category=ODC2DeprecationWarning,
)
def configure_s3_access(
    profile: str | None = None,
    region_name: str = "auto",
    aws_unsigned: bool = False,
    requester_pays: bool = False,
    cloud_defaults: bool = True,
    client=None,
    **gdal_opts,
) -> Credentials | None:
    """
    Use :meth:`datacube.utils.aws.configure_s3_access` instead.
    """
    from datacube.utils.aws import configure_s3_access

    return configure_s3_access(
        profile=profile,
        region_name=region_name,
        aws_unsigned=aws_unsigned,
        requester_pays=requester_pays,
        cloud_defaults=cloud_defaults,
        client=client,
        **gdal_opts,
    )
