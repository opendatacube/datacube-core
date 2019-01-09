# coding=utf-8
"""
Access methods for indexing datasets & products.
"""

import logging

from datacube.config import LocalConfig
from datacube.drivers import index_driver_by_name, index_drivers
from .index import Index

_LOG = logging.getLogger(__name__)


def index_connect(local_config=None, application_name=None, validate_connection=True):
    # type: (LocalConfig, str, bool) -> Index
    """
    Create a Data Cube Index that can connect to a PostgreSQL server

    It contains all the required connection parameters, but doesn't actually
    check that the server is available.

    :param application_name: A short, alphanumeric name to identify this application.
    :param datacube.config.LocalConfig local_config: Config object to use. (optional)
    :param validate_connection: Validate database connection and schema immediately
    :rtype: datacube.index.index.Index
    :raises datacube.drivers.postgres._connections.IndexSetupError:
    """
    if local_config is None:
        local_config = LocalConfig.find()

    driver_name = local_config.get('index_driver', 'default')
    index_driver = index_driver_by_name(driver_name)
    if not index_driver:
        raise RuntimeError(
            "No index driver found for %r. %s available: %s" % (
                driver_name, len(index_drivers()), ', '.join(index_drivers())
            )
        )

    return index_driver.connect_to_index(local_config,
                                         application_name=application_name,
                                         validate_connection=validate_connection)
