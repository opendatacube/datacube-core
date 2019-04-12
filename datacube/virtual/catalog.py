"""
Catalog of virtual products.
"""

from collections.abc import Mapping

class Catalog(Mapping):
    """
    A catalog of virtual products specified in a yaml document.
    """

    def __init__(self, name_resolver, catalog):
        self.name_resolver = name_resolver
        self.catalog = catalog

    def __getitem__(self, key):
        """
        Look up virtual product by name.
        """
        return self.name_resolver.construct(**self.catalog['products'][key]['recipe'])

    def __len__(self):
        return len(self.catalog['products'])

    def __iter__(self):
        return iter(self.catalog['products'])
