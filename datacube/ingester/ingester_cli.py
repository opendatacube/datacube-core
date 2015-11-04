from __future__ import absolute_import
import click as click
import logging

from datacube.ingester.ingester import run_ingest

_LOG = logging.getLogger(__name__)
CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


def setup_logging(verbosity, filename=None):
    """
    Setups up logging, defaults to WARN

    :param verbosity: 1 for INFO, 2 for DEBUG
    :return:
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    stderr_logging_level = logging.WARN - 10 * verbosity
    sh = logging.StreamHandler()
    sh.setLevel(stderr_logging_level)
    sh.setFormatter(formatter)

    logger.addHandler(sh)
    _LOG.debug('Logging to console at level %d' % stderr_logging_level)

    if filename:
        fh = logging.FileHandler(filename)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        _LOG.debug('Logging to %s' % filename)


@click.command(help="Ingest a dataset into a storage unit.", context_settings=CLICK_SETTINGS)
@click.option('--ingest-config', default='ingest_config.yaml',
              type=click.Path(exists=True, readable=True),
              help="Defaults to ./ingest_config.yaml")
@click.option('--storage-config', default='storage_config.yaml',
              type=click.Path(exists=True, readable=True),
              help="Defaults to ./storage_config.yaml")
@click.option('--log', type=click.Path(), help="Log ingest process to given filename")
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.argument('dataset-path', type=click.Path(exists=True, readable=True))
def main(ingest_config, storage_config, dataset_path, log, verbose=0):
    # "/short/u46/gxr547/GA/NBAR/LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228/"
    setup_logging(verbose, log)
    run_ingest(storage_config, ingest_config, dataset_path)
