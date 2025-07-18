# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Catalog of virtual products.
"""

from collections.abc import Iterable, Iterator, Mapping
from itertools import chain
from typing import Any

import yaml
from typing_extensions import override

from datacube.model.utils import SafeDumper


class UnappliedTransform:
    def __init__(self, name_resolver, recipe) -> None:
        self.name_resolver = name_resolver
        self.recipe = recipe

    def __call__(self, input) -> Any:  # noqa: A002
        return self.name_resolver.construct(**self.recipe, input=input)

    @override
    def __repr__(self) -> str:
        return yaml.dump(
            self.recipe, Dumper=SafeDumper, default_flow_style=False, indent=2
        )


class Catalog(Mapping):
    """
    A catalog of virtual products specified in a yaml document.
    """

    def __init__(self, name_resolver, contents) -> None:
        self.name_resolver = name_resolver
        self.contents = contents
        common = set(self._names("products")) & set(self._names("transforms"))
        assert not common, f"common names found in products and transforms {common}"

    def _names(self, section) -> list:
        """List of names under a section (products or transforms)."""
        if section not in self.contents:
            return []
        return list(self.contents[section])

    @override
    def __getitem__(self, name: str):
        """
        Looks up a virtual product or transform by name.
        Returns `None` if not found.
        """
        if name in self._names("products"):
            return self.name_resolver.construct(
                **self.contents["products"][name]["recipe"]
            )
        if name in self._names("transforms"):
            return UnappliedTransform(
                self.name_resolver, self.contents["transforms"][name]["recipe"]
            )

        # raising a `KeyError` here stops autocompletion from working
        return None

    def __getattr__(self, name: str):
        return self[name]

    @override
    def __len__(self) -> int:
        return len(self._names("products")) + len(self._names("transforms"))

    @override
    def __iter__(self) -> Iterator:
        return chain(iter(self._names("products")), iter(self._names("transforms")))

    @override
    def __dir__(self) -> Iterable[str]:
        """
        Override to provide autocompletion of products and transforms.
        """
        return sorted(
            dir(Mapping)
            + list(self.__dict__)
            + self._names("products")
            + self._names("transforms")
        )
