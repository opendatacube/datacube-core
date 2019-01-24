#!/usr/bin/env python
"""
A Simple Data Cube Replication Tool

Connects to a remote Data Cube via SSH, and downloads database records and files to a local file system and database.

Provide a configuration file in ~/.datacube.replication.conf in YAML format, or specify an alternate location
on the command line.

For example, the following config will download 3 PQ products for the specified time and space range. Queries
are specified the same as when using the API to search for datasets.

.. code-block:: yaml

    remote_host: raijin.nci.org.auo
    remote_user: dra547
    db_password: xxxxxxxxxxxx
    remote_dir: /g/data/
    local_dir: C:/datacube/

    replicated_data:
    - product: ls5_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

    - product: ls7_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

    - product: ls8_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

"""

import logging
import os.path
from configparser import ConfigParser
from pathlib import Path

import click
import yaml
from paramiko import SSHClient, WarningPolicy
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

from datacube import Datacube
from datacube.config import LocalConfig, _DEFAULT_CONF
from datacube.index import index_connect
from datacube.ui.click import global_cli_options

LOG = logging.getLogger('simple_replicator')

DEFAULT_REPLICATION_CONFIG = os.path.expanduser('~/.datacube.replication.conf')


def uri_to_path(uri):
    return uri.replace('file://', '')


class DatacubeReplicator(object):
    def __init__(self, config):
        self.remote_host = config['remote_host']
        self.remote_user = config['remote_user']
        self.db_password = config['db_password']
        self.remote_dir = config['remote_dir']
        self.local_dir = config['local_dir']
        self.replication_defns = config['replicated_data']

        self.client = None
        self.sftp = None
        self.tunnel = None
        self.remote_dc_config = None
        self.remote_dc = None
        self.local_index = index_connect()

    def run(self):
        self.connect()
        self.read_remote_config()
        self.connect_to_db()
        self.replicate_all()
        self.disconnect()

    def connect(self):
        client = SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(WarningPolicy())
        client.connect(hostname=self.remote_host, username=self.remote_user)

        LOG.debug(client)
        self.client = client
        self.sftp = client.open_sftp()

    def disconnect(self):
        self.client.close()
        self.tunnel.stop()

    def read_remote_config(self):
        remote_config = ConfigParser()
        remote_config.read_string(_DEFAULT_CONF)
        with self.sftp.open('.datacube.conf') as fin:
            remote_config.read_file(fin)
        self.remote_dc_config = LocalConfig(remote_config)

    def connect_to_db(self):
        self.tunnel = SSHTunnelForwarder(
            self.remote_host,
            ssh_username=self.remote_user,
            remote_bind_address=(self.remote_dc_config.db_hostname, int(self.remote_dc_config.db_port)))
        self.tunnel.start()

        # pylint: disable=protected-access
        self.remote_dc_config._config['datacube']['db_hostname'] = '127.0.0.1'
        self.remote_dc_config._config['datacube']['db_port'] = str(self.tunnel.local_bind_port)
        self.remote_dc_config._config['datacube']['db_username'] = self.remote_user
        self.remote_dc_config._config['datacube']['db_password'] = self.db_password

        # This requires the password from somewhere
        # Parsing it out of .pgpass sounds error prone and fragile
        # Lets put it in the configuration for now
        LOG.debug('Remote configuration loaded %s', self.remote_dc_config)

        self.remote_dc = Datacube(config=self.remote_dc_config)

    def replicate_all(self):

        for defn in tqdm(self.replication_defns, 'Replicating products'):
            self.replicate(defn)

    def replicate_all_products(self):
        products = self.remote_dc.index.products.get_all()
        for product in products:
            self.local_index.products.add(product)

    def replicate(self, defn):
        datasets = list(self.remote_dc.find_datasets(**defn))

        if not datasets:
            LOG.info('No remote datasets found matching %s', defn)
            return

        # TODO: use generator not list
        product = datasets[0].type
        LOG.info('Ensuring remote product is in local index. %s', product)

        self.local_index.products.add(product)

        for dataset in tqdm(datasets, 'Datasets'):
            # dataset = remote_dc.index.datasets.get(dataset.id, include_sources=True)
            # We would need to pull the parent products down too
            # TODO: Include parent source datasets + product definitions
            dataset.sources = {}

            LOG.debug('Replicating dataset %s', dataset)
            remote_path = uri_to_path(dataset.local_uri)
            local_path = self.remote_to_local(uri_to_path(dataset.local_uri))

            # Ensure local path exists
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)

            # Download file
            self.sftp.get(remote_path, local_path)

            # Add to local index
            dataset.local_uri = 'file://' + local_path
            self.local_index.datasets.add(dataset)
            LOG.debug('Downloaded to %s', local_path)

    def remote_to_local(self, remote):
        return remote.replace(self.remote_dir, self.local_dir)


def replicate_data(config):
    replicator = DatacubeReplicator(config)
    replicator.run()


@click.command(help=__doc__)
@click.argument('config_path', required=False)
@global_cli_options
def replicate(config_path):
    """
    Connect to a remote Datacube, and replicate data locally.
    """
    if config_path is None:
        config_path = DEFAULT_REPLICATION_CONFIG
    LOG.debug('Config path: %s', config_path)
    with open(config_path) as fin:
        config = yaml.load(fin)

    replicate_data(config)


if __name__ == '__main__':
    replicate()
