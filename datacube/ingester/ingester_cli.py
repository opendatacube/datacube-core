import click as click

from ingester import run_ingest

__author__ = 'Damien Ayers'
CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(help="Example output filename format: combined_{x}_{y}.nc", context_settings=CLICK_SETTINGS)
@click.option('--ingest-config', default='ingest_config.yaml', type=click.Path(exists=True, readable=True))
@click.option('--storage-config', default='storage_config.yaml', type=click.Path(exists=True, readable=True))
@click.option('--log', type=click.Path())
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.argument('dataset-path',  type=click.Path(exists=True, readable=True))
def main(ingest_config, storage_config, dataset_path):
    # "/short/u46/gxr547/GA/NBAR/LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228/"
    run_ingest(storage_config, ingest_config, dataset_path)


