# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Utility functions
"""
import os
import sys
import logging
from typing import Optional


class DatacubeException(Exception):  # noqa: N818
    """Your Data Cube has malfunctioned"""
    pass


def gen_password(num_random_bytes=12):
    """
    Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')


def report_to_user(msg: str, logger: Optional[logging.Logger] = None, progress_indicator=False):
    """
    Report a message or progress indicator to the user, either via stdout or via the log if stdout
    is not a typewriter.

    :param msg: The message to report to the user.
    :param logger: The logger (optional - if not provided and stdout is not a typewriter, the message is not reported)
    :param progress_indicator: If true, and writing to stdout, no eol character is written and the stream is flushed
                               after writing.
    :return: None
    """
    if sys.stdout.isatty():
        if progress_indicator:
            print(msg, end="", flush=True)
        else:
            print(msg)
    elif logger:
        logger.info(msg)
