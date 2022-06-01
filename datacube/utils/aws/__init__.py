# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Helper methods for working with AWS
"""
import os
import time
import functools
import botocore
import botocore.session
from botocore.credentials import Credentials, ReadOnlyCredentials
from botocore.session import Session
from urllib.request import urlopen
from urllib.parse import urlparse
from sqlalchemy.engine.url import URL

from typing import Optional, Dict, Tuple, Any, Union, IO
from datacube.utils.generic import thread_local_cache

ByteRange = Union[slice, Tuple[int, int]]       # pylint: disable=invalid-name
MaybeS3 = Optional[botocore.client.BaseClient]  # pylint: disable=invalid-name

__all__ = (
    "s3_url_parse",
    "s3_fmt_range",
    "s3_client",
    "s3_open",
    "s3_fetch",
    "s3_dump",
    "ec2_metadata",
    "ec2_current_region",
    "botocore_default_region",
    "auto_find_region",
    "get_creds_with_retry",
    "mk_boto_session",
    "get_aws_settings",
    "configure_s3_access",
)


def _fetch_text(url: str, timeout: float = 0.1) -> Optional[str]:
    try:
        with urlopen(url, timeout=timeout) as resp:
            if 200 <= resp.getcode() < 300:
                return resp.read().decode('utf8')
            else:
                return None
    except IOError:
        return None


def s3_url_parse(url: str) -> Tuple[str, str]:
    """ Return Bucket, Key tuple
    """
    uu = urlparse(url)
    if uu.scheme != "s3":
        raise ValueError("Not a valid s3 url")
    return uu.netloc, uu.path.lstrip('/')


def s3_fmt_range(r: Optional[ByteRange]):
    """ None -> None
        (in, out) -> "bytes={in}-{out-1}"
    """
    if r is None:
        return None

    if isinstance(r, slice):
        if r.step not in [1, None]:
            raise ValueError("Can not process decimated slices")
        if r.stop is None:
            raise ValueError("Can not process open ended slices")

        _in = 0 if r.start is None else r.start
        _out = r.stop
    else:
        _in, _out = r

    if _in < 0 or _out < 0:
        raise ValueError("Slice has to be positive")

    return 'bytes={:d}-{:d}'.format(_in, _out-1)


def ec2_metadata(timeout: float = 0.1) -> Optional[Dict[str, Any]]:
    """ When running inside AWS returns dictionary describing instance identity.
        Returns None when not inside AWS
    """
    import json
    txt = _fetch_text('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout)

    if txt is None:
        return None

    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None


def ec2_current_region() -> Optional[str]:
    """ Returns name of the region  this EC2 instance is running in.
    """
    cfg = ec2_metadata()
    if cfg is None:
        return None
    return cfg.get('region', None)


def botocore_default_region(session: Optional[Session] = None) -> Optional[str]:
    """ Returns default region name as configured on the system.
    """
    if session is None:
        session = botocore.session.get_session()
    return session.get_config_variable('region')


def auto_find_region(session: Optional[Session] = None, default: Optional[str] = None) -> str:
    """
    Try to figure out which region name to use

    1. Region as configured for this/default session
    2. Region this EC2 instance is running in
    3. Value supplied in `default`
    4. raise exception
    """
    region_name = botocore_default_region(session)

    if region_name is None:
        region_name = ec2_current_region()

    if region_name is not None:
        return region_name

    if default is None:
        raise ValueError('Region name is not supplied and default can not be found')

    return default


def get_creds_with_retry(session: Session,
                         max_tries: int = 10,
                         sleep: float = 0.1) -> Optional[Credentials]:
    """ Attempt to obtain credentials upto `max_tries` times with back off
    :param session: botocore session, see mk_boto_session
    :param max_tries: number of attempt before failing and returing None
    :param sleep: number of seconds to sleep after first failure (doubles on every consecutive failure)
    """
    for i in range(max_tries):
        if i > 0:
            time.sleep(sleep)
            sleep = min(sleep*2, 10)

        creds = session.get_credentials()
        if creds is not None:
            return creds

    return None


def mk_boto_session(profile: Optional[str] = None,
                    creds: Optional[ReadOnlyCredentials] = None,
                    region_name: Optional[str] = None) -> Session:
    """ Get botocore session with correct `region` configured

    :param profile: profile name to lookup
    :param creds: Override credentials with supplied data
    :param region_name: default region_name to use if not configured for a given profile
    """
    session = botocore.session.Session(profile=profile)

    if creds is not None:
        session.set_credentials(creds.access_key,
                                creds.secret_key,
                                creds.token)

    _region = session.get_config_variable("region")
    if _region is None:
        if region_name is None or region_name == "auto":
            _region = auto_find_region(session, default='us-west-2')
        else:
            _region = region_name
        session.set_config_variable("region", _region)

    return session


def _s3_cache_key(profile: Optional[str] = None,
                  creds: Optional[ReadOnlyCredentials] = None,
                  region_name: Optional[str] = None,
                  aws_unsigned: bool = False,
                  prefix: str = "s3") -> str:
    parts = [prefix,
             "" if creds is None else creds.access_key,
             "T" if aws_unsigned else "F",
             profile or "",
             region_name or ""]
    return ":".join(parts)


def _mk_s3_client(profile: Optional[str] = None,
                  creds: Optional[ReadOnlyCredentials] = None,
                  region_name: Optional[str] = None,
                  session: Optional[Session] = None,
                  use_ssl: bool = True,
                  **cfg) -> botocore.client.BaseClient:
    """ Construct s3 client with configured region_name.

    :param profile    : profile name to lookup (only used if session is not supplied)
    :param creds      : Override credentials with supplied data
    :param region_name: region_name to use, overrides session setting
    :param session    : botocore session to use
    :param use_ssl    : Whether to connect via http or https
    :param cfg        : passed on to ``botocore.client.Config(..)``
                        max_pool_connections
                        connect_timeout
                        read_timeout
                        parameter_validation
                        ...
    """
    if session is None:
        session = mk_boto_session(profile=profile,
                                  creds=creds,
                                  region_name=region_name)

    extras = {}  # type: Dict[str, Any]
    if creds is not None:
        extras.update(aws_access_key_id=creds.access_key,
                      aws_secret_access_key=creds.secret_key,
                      aws_session_token=creds.token)
    if region_name is not None:
        extras['region_name'] = region_name

    return session.create_client('s3',
                                 use_ssl=use_ssl,
                                 **extras,
                                 config=botocore.client.Config(**cfg))


def _aws_unsigned_check_env() -> bool:
    def parse_bool(v: str) -> bool:
        return v.upper() in ('YES', 'Y', 'TRUE', 'T', '1')

    for evar in ('AWS_UNSIGNED', 'AWS_NO_SIGN_REQUEST'):
        v = os.environ.get(evar, None)
        if v is not None:
            return parse_bool(v)

    return False


def s3_client(profile: Optional[str] = None,
              creds: Optional[ReadOnlyCredentials] = None,
              region_name: Optional[str] = None,
              session: Optional[Session] = None,
              aws_unsigned: Optional[bool] = None,
              use_ssl: bool = True,
              cache: Union[bool, str] = False,
              **cfg) -> botocore.client.BaseClient:
    """ Construct s3 client with configured region_name.

    :param profile: profile name to lookup (only used if session is not supplied)
    :param creds: Override credentials with supplied data
    :param region_name: region_name to use, overrides session setting
    :param aws_unsigned: Do not use any credentials when accessing S3 resources
    :param session: botocore session to use
    :param use_ssl: Whether to connect via http or https
    :param cache: ``True`` - store/lookup s3 client in thread local cache.
                  ``"purge"`` - delete from cache and return what was there to begin with

    :param cfg: passed on to ``botocore.client.Config(..)``

    """
    if aws_unsigned is None:
        if creds is None:
            aws_unsigned = _aws_unsigned_check_env()
        else:
            aws_unsigned = False

    if aws_unsigned:
        cfg.update(signature_version=botocore.UNSIGNED)

    if not cache:
        return _mk_s3_client(profile,
                             creds=creds,
                             region_name=region_name,
                             session=session,
                             use_ssl=use_ssl,
                             **cfg)

    _cache = thread_local_cache("__aws_s3_cache", {})

    key = _s3_cache_key(profile=profile,
                        region_name=region_name,
                        creds=creds,
                        aws_unsigned=aws_unsigned)

    if cache == "purge":
        return _cache.pop(key, None)

    s3 = _cache.get(key, None)

    if s3 is None:
        s3 = _mk_s3_client(profile,
                           creds=creds,
                           region_name=region_name,
                           session=session,
                           use_ssl=use_ssl,
                           **cfg)
        _cache[key] = s3

    return s3


def s3_open(url: str,
            s3: MaybeS3 = None,
            range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
            **kwargs):
    """ Open whole or part of S3 object

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
    :param kwargs: are passed on to ``s3.get_object(..)``
    """
    if range is not None:
        try:
            kwargs['Range'] = s3_fmt_range(range)
        except Exception:
            raise ValueError('Bad range passed in: ' + str(range))

    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)
    oo = s3.get_object(Bucket=bucket, Key=key, **kwargs)  # type: ignore[attr-defined]
    return oo['Body']


def s3_head_object(url: str,
                   s3: MaybeS3 = None,
                   **kwargs) -> Optional[Dict[str, Any]]:
    """
    Head object, return object metadata.

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param kwargs: are passed on to ``s3.head_object(..)``
    """
    from botocore.exceptions import ClientError

    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)

    try:
        oo = s3.head_object(Bucket=bucket, Key=key, **kwargs)  # type: ignore[attr-defined]
    except ClientError:
        return None

    meta = oo.pop('ResponseMetadata', {})
    code = meta.get('HTTPStatusCode', 0)
    if 200 <= code < 300:
        return oo

    # it actually raises exceptions when http code is in the "fail" range
    return None  # pragma: no cover


def s3_fetch(url: str,
             s3: MaybeS3 = None,
             range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
             **kwargs) -> bytes:
    """ Read entire or part of object into memory and return as bytes

    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see make_s3_client()
    :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
    """
    return s3_open(url, s3=s3, range=range, **kwargs).read()


def s3_dump(data: Union[bytes, str, IO],
            url: str,
            s3: MaybeS3 = None,
            **kwargs):
    """ Write data to s3 object.

    :param data: bytes to write
    :param url: s3://bucket/path/to/object
    :param s3: pre-configured s3 client, see s3_client()
    :param kwargs: Are passed on to ``s3.put_object(..)``

    ContentType
    ACL
    """

    s3 = s3 or s3_client()
    bucket, key = s3_url_parse(url)

    r = s3.put_object(Bucket=bucket,  # type: ignore[attr-defined]
                      Key=key,
                      Body=data,
                      **kwargs)
    code = r['ResponseMetadata']['HTTPStatusCode']
    return 200 <= code < 300


def get_aws_settings(profile: Optional[str] = None,
                     region_name: str = "auto",
                     aws_unsigned: bool = False,
                     requester_pays: bool = False) -> Tuple[Dict[str, Any], Optional[Credentials]]:
    """
    Compute ``aws=`` parameter for ``set_default_rio_config``.

    see also ``datacube.utils.rio.set_default_rio_config``

    Returns a tuple of: ``(aws: Dictionary, creds: session credentials from botocore)``.

    Note that credentials are baked in to ``aws`` setting dictionary,
    however since those might be STS credentials they might require refresh
    hence they are returned from this function separately as well.
    """
    session = mk_boto_session(profile=profile,
                              region_name=region_name)

    region_name = session.get_config_variable("region")

    if aws_unsigned:
        return (dict(region_name=region_name,
                     aws_unsigned=True), None)

    creds = get_creds_with_retry(session)
    if creds is None:
        raise ValueError("Couldn't get credentials")

    cc = creds.get_frozen_credentials()

    return (dict(region_name=region_name,
                 aws_access_key_id=cc.access_key,
                 aws_secret_access_key=cc.secret_key,
                 aws_session_token=cc.token,
                 requester_pays=requester_pays), creds)


def obtain_new_iam_auth_token(url: URL, region_name: str = "auto", profile_name: Optional[str] = None) -> str:
    # Boto3 is not core requirement, but ImportError is probably the right exception to throw anyway.
    from boto3.session import Session as Boto3Session

    session = Boto3Session(profile_name=profile_name)
    client = session.client("rds", region_name=region_name)
    return client.generate_db_auth_token(DBHostname=url.host, Port=url.port, DBUsername=url.username,
                                         Region=region_name)


def configure_s3_access(profile=None,
                        region_name="auto",
                        aws_unsigned=False,
                        requester_pays=False,
                        cloud_defaults=True,
                        client=None,
                        **gdal_opts):
    """ Credentialize for S3 bucket access or configure public access.

    This function obtains credentials for S3 access and passes them on to
    processing threads, either local or on dask cluster.


    .. note::

       if credentials are STS based they will eventually expire, currently
       this case is not handled very well, reads will just start failing
       eventually and will never recover.

    :param profile:        AWS profile name to use
    :param region_name:    Default region_name to use if not configured for a given/default AWS profile
    :param aws_unsigned:   If ``True`` don't bother with credentials when reading from S3
    :param requester_pays: Needed when accessing requester pays buckets

    :param cloud_defaults: Assume files are in the cloud native format, i.e. no side-car files, disables
                           looking for side-car files, makes things faster but won't work for files
                           that do have side-car files with extra metadata.

    :param client:         Dask distributed ``dask.Client`` instance, if supplied apply settings on the
                           dask cluster rather than locally.
    :param gdal_opts:      Any other option to pass to GDAL environment setup

    :returns: credentials object or ``None`` if ``aws_unsigned=True``
    """
    from ..rio import set_default_rio_config

    aws, creds = get_aws_settings(profile=profile,
                                  region_name=region_name,
                                  aws_unsigned=aws_unsigned,
                                  requester_pays=requester_pays)

    if client is None:
        set_default_rio_config(aws=aws, cloud_defaults=cloud_defaults, **gdal_opts)
    else:
        client.register_worker_callbacks(
            functools.partial(set_default_rio_config,
                              aws=aws,
                              cloud_defaults=cloud_defaults,
                              **gdal_opts))

    return creds
