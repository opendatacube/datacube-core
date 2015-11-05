# coding=utf-8
"""
Module
"""
from __future__ import absolute_import
import logging

from pathlib import Path

from datacube.ingest import ingest


if __name__ == '__main__':
    # TODO: Command-line script (move from storage package & expand)
    import sys

    logging.basicConfig()
    ingest(Path(sys.argv[1]))
