"""
Catalog of virtual products.
"""

from collections.abc import Mapping
from itertools import chain

import yaml

from datacube.model.utils import SafeDumper


class UnappliedTransform:
    def __init__(self, name_resolver, recipe):
        self.name_resolver = name_resolver
        self.recipe = recipe

    def __call__(self, input):
        return self.name_resolver.construct(**self.recipe, input=input)

    def __repr__(self):
        return yaml.dump(self.recipe, Dumper=SafeDumper,
                         default_flow_style=False, indent=2)


class Catalog(Mapping):
    """
    A catalog of virtual products specified in a yaml document.
    """

    def __init__(self, name_resolver, contents):
        self.name_resolver = name_resolver
        self.contents = contents
        common = set(self._names('products')) & set(self._names('transforms'))
        assert not common, f"common names found in products and transforms {common}"

    def _names(self, section):
        """ List of names under a section (products or transforms). """
        if section not in self.contents:
            return []
        return list(self.contents[section])

    def __getitem__(self, name):
        """
        Looks up a virtual product or transform by name.
        Returns `None` if not found.
        """
        if name in self._names('products'):
            return self.name_resolver.construct(**self.contents['products'][name]['recipe'])
        if name in self._names('transforms'):
            return UnappliedTransform(self.name_resolver, self.contents['transforms'][name]['recipe'])

        # raising a `KeyError` here stops autocompletion from working
        return None

    def __getattr__(self, name):
        return self[name]

    def __len__(self):
        return len(self._names('products')) + len(self._names('transforms'))

    def __iter__(self):
        return chain(iter(self._names('products')), iter(self._names('transforms')))

    def __dir__(self):
        """
        Override to provide autocompletion of products and transforms.
        """
        return sorted(dir(Mapping) + list(self.__dict__) + self._names('products') + self._names('transforms'))
