"""
Helper methods for working with AWS
"""
import botocore
import botocore.session
from urllib.request import urlopen


def _fetch_text(url, timeout=0.1):
    try:
        with urlopen(url, timeout=timeout) as resp:
            if 200 <= resp.getcode() < 300:
                return resp.read().decode('utf8')
            else:
                return None
    except IOError:
        return None


def ec2_metadata(timeout=0.1):
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


def ec2_current_region():
    """ Returns name of the region  this EC2 instance is running in.
    """
    cfg = ec2_metadata()
    if cfg is None:
        return None
    return cfg.get('region', None)


def botocore_default_region(session=None):
    """ Returns default region name as configured on the system.
    """
    if session is None:
        session = botocore.session.get_session()
    return session.get_config_variable('region')


def auto_find_region(session=None):
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
