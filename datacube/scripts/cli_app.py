#!/usr/bin/env python
# coding=utf-8
"""
Datacube command-line interface
"""

from __future__ import absolute_import

from datacube.ui.click import cli
import datacube.scripts.dataset
import datacube.scripts.ingest
import datacube.scripts.product
import datacube.scripts.metadata_type
import datacube.scripts.system
import datacube.scripts.user
import datacube.scripts.product_extent


if __name__ == '__main__':
    cli()
