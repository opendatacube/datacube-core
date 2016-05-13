#!/usr/bin/env python
# coding=utf-8
"""
Datacube command-line interface
"""

from __future__ import absolute_import

from datacube.ui.click import cli
import datacube.scripts.index
import datacube.scripts.ingest


if __name__ == '__main__':
    cli()
