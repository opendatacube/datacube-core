OPTIONS = {'reproject_threads': 4}


#: pylint: disable=invalid-name
class set_options(object):
    """Set global state within a controlled context

    Currently, the only supported options are:
    * reproject_threads: The number of threads to use when reprojecting

    You can use ``set_options`` either as a context manager::

        with datacube.set_options(reproject_threads=16):
            ...

    Or to set global options::

        datacube.set_options(reproject_threads=16)
    """
    def __init__(self, **kwargs):
        self.old = OPTIONS.copy()
        OPTIONS.update(kwargs)

    def __enter__(self):
        return

    def __exit__(self, exc_type, value, traceback):
        OPTIONS.clear()
        OPTIONS.update(self.old)
