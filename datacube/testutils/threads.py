# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" threads related stuff
"""

from concurrent.futures import Future
from functools import partial


class FakeThreadPoolExecutor():
    """ Limited version of ThreadPool that executes in the current thread.
    """

    def submit(self, fn, *args, **kwargs):
        f = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as e:  # pylint: disable=broad-except
            f.set_exception(e)

        return f

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        return map(partial(self.submit, fn), *iterables)

    def shutdown(self, wait=True):
        pass
