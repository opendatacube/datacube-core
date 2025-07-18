# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import copy
from collections.abc import Mapping
from typing import Any, cast

from datacube.model import Measurement
from datacube.utils import import_function
from datacube.utils.documents import parse_yaml

from .catalog import Catalog
from .impl import (
    Transformation,
    VirtualProduct,
    VirtualProductException,
    from_validated_recipe,
    virtual_product_kind,
)
from .transformations import (
    ApplyMask,
    Expressions,
    MakeMask,
    Rename,
    Select,
    ToFloat,
    XarrayReduction,
    day,
    earliest_time,
    fiscal_year,
    month,
    week,
    year,
)
from .utils import reject_keys

__all__ = ["Measurement", "Transformation", "construct"]


class NameResolver:
    """Apply a mapping from name to callable objects in a recipe."""

    def __init__(self, lookup_table) -> None:
        self.lookup_table = lookup_table

    def clone(self) -> "NameResolver":
        """Safely copy the resolver in order to possibly extend it."""
        return NameResolver(copy.deepcopy(self.lookup_table))

    @staticmethod
    def _assert(cond, msg) -> None:
        if not cond:
            raise VirtualProductException(msg)

    def construct(self, **recipe) -> VirtualProduct:
        """Validate recipe and construct virtual product."""

        get = recipe.get

        def lookup(name, namespace=None, kind: str = "transformation"):
            if callable(name):
                return name

            if (
                namespace is not None
                and namespace in self.lookup_table
                and name in self.lookup_table[namespace]
            ):
                result = self.lookup_table[namespace][name]
            else:
                try:
                    result = import_function(name)
                except (ImportError, AttributeError):
                    msg = f"could not resolve {kind} {name} in {recipe}"
                    raise VirtualProductException(msg) from None

            self._assert(callable(result), f"{kind} not callable in {recipe}")

            return result

        kind = virtual_product_kind(recipe)

        if kind == "product":
            func_keys = ["fuse_func", "dataset_predicate"]
            return from_validated_recipe(
                {
                    key: value
                    if key not in func_keys
                    else lookup(value, kind="function")
                    for key, value in recipe.items()
                }
            )

        if kind == "transform":
            cls_name = recipe["transform"]
            input_product = cast(Mapping, get("input"))

            self._assert(
                input_product is not None, f"no input for transformation in {recipe}"
            )

            return from_validated_recipe(
                dict(
                    transform=lookup(cls_name, "transform"),
                    input=self.construct(**input_product),
                    **reject_keys(recipe, ["transform", "input"]),
                )
            )

        if kind == "collate":
            self._assert(
                len(recipe["collate"]) > 0, f"no children for collate in {recipe}"
            )

            return from_validated_recipe(
                dict(
                    collate=[self.construct(**child) for child in recipe["collate"]],
                    **reject_keys(recipe, ["collate"]),
                )
            )

        if kind == "juxtapose":
            self._assert(
                len(recipe["juxtapose"]) > 0, f"no children for juxtapose in {recipe}"
            )

            return from_validated_recipe(
                dict(
                    juxtapose=[
                        self.construct(**child) for child in recipe["juxtapose"]
                    ],
                    **reject_keys(recipe, ["juxtapose"]),
                )
            )

        if kind == "aggregate":
            cls_name = recipe["aggregate"]
            input_product = cast(Mapping, get("input"))
            group_by = get("group_by")

            self._assert(
                input_product is not None, f"no input for aggregate in {recipe}"
            )
            self._assert(group_by is not None, f"no group_by for aggregate in {recipe}")

            return from_validated_recipe(
                dict(
                    aggregate=lookup(cls_name, "aggregate"),
                    group_by=lookup(group_by, "aggregate/group_by", kind="group_by"),
                    input=self.construct(**input_product),
                    **reject_keys(recipe, ["aggregate", "input", "group_by"]),
                )
            )

        if kind == "reproject":
            input_product = cast(Mapping, get("input"))
            output_crs = recipe["reproject"].get("output_crs")
            resolution = recipe["reproject"].get("resolution")

            self._assert(
                input_product is not None, f"no input for reproject in {recipe}"
            )
            self._assert(
                output_crs is not None, f"no output_crs for reproject in {recipe}"
            )
            self._assert(
                resolution is not None, f"no resolution for reproject in {recipe}"
            )

            return from_validated_recipe(
                dict(
                    reproject=recipe["reproject"],
                    input=self.construct(**input_product),
                    **reject_keys(recipe, ["reproject", "input"]),
                )
            )

        raise VirtualProductException(
            f"could not understand virtual product recipe: {recipe}"
        )

    def register(self, namespace: str, name: str, callable_obj) -> None:
        """Register a callable to the name resolver."""
        if namespace not in self.lookup_table:
            self.lookup_table[namespace] = {}

        if name in self.lookup_table[namespace]:
            raise VirtualProductException(
                f"name {name} under {namespace} is already registered"
            )

        self.lookup_table[namespace][name] = callable_obj


DEFAULT_RESOLVER = NameResolver(
    {
        "transform": {
            "make_mask": MakeMask,
            "apply_mask": ApplyMask,
            "to_float": ToFloat,
            "rename": Rename,
            "select": Select,
            "expressions": Expressions,
        },
        "aggregate": {"xarray_reduction": XarrayReduction},
        "aggregate/group_by": {
            "year": year,
            "month": month,
            "week": week,
            "day": day,
            "earliest_time": earliest_time,
            "fiscal_year": fiscal_year,
        },
    }
)


def construct(
    name_resolver: NameResolver | None = None, **recipe: Mapping[str, Any]
) -> VirtualProduct:
    """
    Create a virtual product from a specification dictionary.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return name_resolver.construct(**recipe)


def construct_from_yaml(
    recipe: str, name_resolver: NameResolver | None = None
) -> VirtualProduct:
    """
    Create a virtual product from a yaml recipe.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return construct(name_resolver=name_resolver, **parse_yaml(recipe))


def catalog_from_yaml(
    catalog_body: str, name_resolver: NameResolver | None = None
) -> Catalog:
    """
    Load a catalog of virtual products from a yaml document.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return Catalog(name_resolver, parse_yaml(catalog_body))


def catalog_from_file(filename: str, name_resolver=None) -> Catalog:
    """
    Load a catalog of virtual products from a yaml file.
    """
    with open(filename) as fl:
        return catalog_from_yaml(fl.read(), name_resolver=name_resolver)
