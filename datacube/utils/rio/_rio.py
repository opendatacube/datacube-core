""" rasterio environment management tools
"""
import threading
from types import SimpleNamespace
import functools
import rasterio
from rasterio.session import AWSSession, DummySession
import rasterio.env
from datacube.utils.generic import thread_local_cache

_CFG_LOCK = threading.Lock()
_CFG = SimpleNamespace(aws=None,
                       cloud_defaults=False,
                       kwargs={},
                       epoch=0)


SECRET_KEYS = ('AWS_ACCESS_KEY_ID',
               'AWS_SECRET_ACCESS_KEY',
               'AWS_SESSION_TOKEN')


def _sanitize(opts, keys):
    return {k: (v if k not in keys
                else 'xx..xx')
            for k, v in opts.items()}


def _state(purge=False):
    """
    .env   None| rasterio.Env
    .epoch -1  | +Int
    """
    return thread_local_cache("__rio_state__",
                              SimpleNamespace(env=None, epoch=-1),
                              purge=purge)


def get_rio_env(sanitize=True):
    """ Get GDAL params configured by rasterio for the current thread.

    :param sanitize: If True replace sensitive Values with 'x'
    """

    env = rasterio.env.local._env  # pylint: disable=protected-access
    if env is None:
        return {}
    opts = env.get_config_options()
    if sanitize:
        opts = _sanitize(opts, SECRET_KEYS)

    return opts


def deactivate_rio_env():
    """ Exit previously configured environment, or do nothing if one wasn't configured.
    """
    state = _state(purge=True)

    if state.env is not None:
        state.env.__exit__(None, None, None)


def activate_rio_env(aws=None, cloud_defaults=False, **kwargs):
    """ Inject activated rasterio.Env into current thread.

    This de-activates previously setup environment.

    :param aws: Dictionary of options for rasterio.session.AWSSession
                OR 'auto' -- session = rasterio.session.AWSSession()

    :param cloud_defaults: When True inject settings for reading COGs
    :param **kwargs: Passed on to rasterio.Env(..) constructor
    """
    session = DummySession()

    if aws is not None:
        if not (aws == 'auto' or
                isinstance(aws, dict)):
            raise ValueError('Only support: None|"auto"|{..} for `aws` parameter')

        aws = {} if aws == 'auto' else dict(**aws)
        region_name = aws.get('region_name', 'auto')

        if region_name == 'auto':
            from datacube.utils.aws import auto_find_region
            try:
                aws['region_name'] = auto_find_region()
            except ValueError as e:
                # only treat it as error if it was requested by user
                if 'region_name' in aws:
                    raise e

        session = AWSSession(**aws)

    opts = dict(
        GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
        GDAL_HTTP_MAX_RETRY='10',
        GDAL_HTTP_RETRY_DELAY='0.5',
    ) if cloud_defaults else {}

    opts.update(**kwargs)

    state = _state()

    if state.env is not None:
        state.env.__exit__(None, None, None)

    env = rasterio.Env(session=session, **opts)
    env.__enter__()
    state.env = env
    state.epoch = -1

    return get_rio_env()


def activate_from_config():
    """ Check if this threads needs to reconfigure, then does reconfigure.

    - Does nothing if this thread is already configured and configuration hasn't changed.
    - Configures current thread with default rio settings
    """
    cfg = _CFG
    state = _state()

    if cfg.epoch != state.epoch:
        ee = activate_rio_env(aws=cfg.aws,
                              cloud_defaults=cfg.cloud_defaults,
                              **cfg.kwargs)
        state.epoch = cfg.epoch
        return ee

    return None


def set_default_rio_config(aws=None, cloud_defaults=False, **kwargs):
    """ Setup default configuration for rasterio/GDAL.

    Doesn't actually activate one, just stores configuration for future
    use from IO threads.

    :param aws: Dictionary of options for rasterio.session.AWSSession
                OR 'auto' -- session = rasterio.session.AWSSession()

    :param cloud_defaults: When True inject settings for reading COGs
    :param **kwargs: Passed on to rasterio.Env(..) constructor
    """
    global _CFG  # pylint: disable=global-statement

    with _CFG_LOCK:
        _CFG = SimpleNamespace(aws=aws,
                               cloud_defaults=cloud_defaults,
                               kwargs=kwargs,
                               epoch=_CFG.epoch + 1)


def configure_s3_access(profile=None,
                        region_name="auto",
                        aws_unsigned=False,
                        requester_pays=False,
                        cloud_defaults=True,
                        client=None,
                        **gdal_opts):
    """ Credentialize for S3 bucket access.

    This function obtains credentials for S3 access and passes them on to
    processing threads, either local or on dask cluster.

    @returns credentials object or None if `aws_unsigned=True`

    NOTE: if credentials are STS based they will eventually expire, currently
    this case is not handled very well, reads will just start failing
    eventually and will never recover.

    :param profile:        AWS profile name to use
    :param region_name:    Default region_name to use if not configured for a given/default AWS profile
    :param aws_unsigned:   if True don't bother with credentials when reading from S3
    :param requester_pays: needed when accessing requester pays buckets

    :param cloud_defaults: Assume files are in the cloud native format, i.e. no side-car files, disables
                           looking for side-car files, makes things faster but won't work for files
                           that do have side-car files with extra metadata.

    :param client:         Dask distributed `Client` instance, if supplied apply settings on the dask cluster
                           rather than locally
    """
    from datacube.utils.aws import get_aws_settings

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
