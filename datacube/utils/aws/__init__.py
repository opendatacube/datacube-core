"""
Helper methods for working with AWS
"""
import botocore
import botocore.session
from botocore.credentials import Credentials, ReadOnlyCredentials
from botocore.session import Session
import time
from urllib.request import urlopen
from urllib.parse import urlparse
from typing import Optional, Dict, Tuple, Any, Union


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


def s3_fmt_range(r: Optional[Union[slice, Tuple[int, int]]]):
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


def auto_find_region(session: Optional[Session] = None) -> str:
    """
    Try to figure out which region name to use

    1. Region as configured for this/default session
    2. Region this EC2 instance is running in
    3. None
    """
    region_name = botocore_default_region(session)

    if region_name is None:
        region_name = ec2_current_region()

    if region_name is None:
        raise ValueError('Region name is not supplied and default can not be found')

    return region_name


def get_creds_with_retry(session: Session,
                         max_tries: int = 10,
                         sleep: float = 0.1) -> Optional[Credentials]:
    """ Attempt to obtain credentials upto `max_tries` times with back off
    :param session: botocore session, see get_boto_session
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
            _region = auto_find_region(session)
        else:
            _region = region_name
        session.set_config_variable("region", _region)

    return session


def get_aws_settings(profile: Optional[str] = None,
                     region_name: str = "auto",
                     aws_unsigned: bool = False,
                     requester_pays: bool = False) -> Tuple[Dict[str, Any], Credentials]:
    """Compute `aws=` parameter for `set_default_rio_config`

    see also `datacube.utils.rio.set_default_rio_config`

    Returns a tuple of (aws: Dictionary, creds: session credentials from
    botocore). Note that credentials are baked in to `aws` setting dictionary,
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
