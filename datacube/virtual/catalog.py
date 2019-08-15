"""
Catalog of virtual products.
"""

from collections.abc import Mapping


class Catalog(Mapping):
    """
    A catalog of virtual products specified in a yaml document.
    """

    def __init__(self, name_resolver, contents):
        self.name_resolver = name_resolver
        self.contents = contents

    def __getitem__(self, product_name):
        """
        Look up virtual product by name.
        """
        return self.name_resolver.construct(**self.contents['products'][product_name]['recipe'])

    def __len__(self):
        return len(self.contents['products'])

    def __iter__(self):
        return iter(self.contents['products'])

    def describe(self, product_name):
        """
        Section describing the product in the catalog.
        """
        return self.contents['products'][product_name]
