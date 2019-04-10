#!/usr/bin/env python
# coding=utf-8
"""
Datacube command-line interface
"""


from datacube.ui.click import cli
import datacube.scripts.dataset
import datacube.scripts.ingest
import datacube.scripts.product
import datacube.scripts.metadata
import datacube.scripts.system
import datacube.scripts.user


if __name__ == '__main__':
    cli()
