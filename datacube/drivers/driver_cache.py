from __future__ import absolute_import, print_function
from pkg_resources import iter_entry_points


def load_drivers(group):
    """
    Load available drivers for a given group name.

    :param str group: Name of the entry point group e.g. "datacube.plugins.io.read"

    :returns: Dictionary String -> Driver Object
    """
    def safe_load(ep):
        # pylint: disable=bare-except
        try:
            driver_init = ep.resolve()
        except:
            print('WARNING: failed to resolve {}'.format(ep.name))  # TODO: use proper logger
            return None

        try:
            driver = driver_init()
        except:
            print('WARNING: exception during driver init, {}'.format(ep.name))  # TODO: use proper logger
            return None

        if driver is None:
            print('WARNING: driver init returned None, {}'.format(ep.name))  # TODO: use proper logger

        return driver

    def resolve_all(group):
        for ep in iter_entry_points(group=group, name=None):
            driver = safe_load(ep)
            if driver is not None:
                yield (ep.name, driver)

    return dict((name, driver) for name, driver in resolve_all(group))
