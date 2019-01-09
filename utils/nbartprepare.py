# coding=utf-8
"""
Ingest data from the command-line.
"""

import logging
from pathlib import Path
import yaml
import click
from osgeo import gdal, osr


def prepare_dataset(document):
    document['grid_spatial'] = document['lineage']['source_datasets']['ortho']['grid_spatial']
    # TODO: document['grid_spatial']['projection']['spatial_reference'] = 'zxdfsdfsd'
    return document


@click.command(help="Prepare MODIS datasets for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        if path.is_dir():
            path = path / 'ga-metadata.yaml'
        elif path.suffix != '.yaml':
            raise RuntimeError('want yaml')

        with open(str(path)) as stream:
            documents = list(yaml.load_all(stream))

        documents = [prepare_dataset(dataset) for dataset in documents]
        if documents:
            yaml_path = str(path.parent.joinpath('agdc-metadata.yaml'))
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
