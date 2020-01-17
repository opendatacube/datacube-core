from typing import Mapping, Any
import copy

from .impl import VirtualProduct, Transformation, VirtualProductException
from .impl import from_validated_recipe, virtual_product_kind
from .transformations import MakeMask, ApplyMask, ToFloat, Rename, Select, Expressions
from .transformations import XarrayReduction, year, month, week, day, earliest_time
from .catalog import Catalog
from .utils import reject_keys

from datacube.model import Measurement
from datacube.utils import import_function
from datacube.utils.documents import parse_yaml

__all__ = ['construct', 'Transformation', 'Measurement']


class NameResolver:
    """ Apply a mapping from name to callable objects in a recipe. """

    def __init__(self, lookup_table):
        self.lookup_table = lookup_table

    def clone(self):
        """ Safely copy the resolver in order to possibly extend it. """
        return NameResolver(copy.deepcopy(self.lookup_table))

    @staticmethod
    def _assert(cond, msg):
        if not cond:
            raise VirtualProductException(msg)

    def construct(self, **recipe) -> VirtualProduct:
        """ Validate recipe and construct virtual product. """

        get = recipe.get

        def lookup(name, namespace=None, kind='transformation'):
            if callable(name):
                return name

            if namespace is not None and namespace in self.lookup_table and name in self.lookup_table[namespace]:
                result = self.lookup_table[namespace][name]
            else:
                try:
                    result = import_function(name)
                except (ImportError, AttributeError):
                    msg = "could not resolve {} {} in {}".format(kind, name, recipe)
                    raise VirtualProductException(msg)

            self._assert(callable(result), "{} not callable in {}".format(kind, recipe))

            return result

        kind = virtual_product_kind(recipe)

        if kind == 'product':
            func_keys = ['fuse_func', 'dataset_predicate']
            return from_validated_recipe({key: value if key not in func_keys else lookup(value, kind='function')
                                          for key, value in recipe.items()})

        if kind == 'transform':
            cls_name = recipe['transform']
            input_product = get('input')

            self._assert(input_product is not None, "no input for transformation in {}".format(recipe))

            return from_validated_recipe(dict(transform=lookup(cls_name, 'transform'),
                                              input=self.construct(**input_product),
                                              **reject_keys(recipe, ['transform', 'input'])))

        if kind == 'collate':
            self._assert(len(recipe['collate']) > 0, "no children for collate in {}".format(recipe))

            return from_validated_recipe(dict(collate=[self.construct(**child) for child in recipe['collate']],
                                              **reject_keys(recipe, ['collate'])))

        if kind == 'juxtapose':
            self._assert(len(recipe['juxtapose']) > 0, "no children for juxtapose in {}".format(recipe))

            return from_validated_recipe(dict(juxtapose=[self.construct(**child) for child in recipe['juxtapose']],
                                              **reject_keys(recipe, ['juxtapose'])))

        if kind == 'aggregate':
            cls_name = recipe['aggregate']
            input_product = get('input')
            group_by = get('group_by')

            self._assert(input_product is not None, "no input for aggregate in {}".format(recipe))
            self._assert(group_by is not None, "no group_by for aggregate in {}".format(recipe))

            return from_validated_recipe(dict(aggregate=lookup(cls_name, 'aggregate'),
                                              group_by=lookup(group_by, 'aggregate/group_by', kind='group_by'),
                                              input=self.construct(**input_product),
                                              **reject_keys(recipe, ['aggregate', 'input', 'group_by'])))

        if kind == 'reproject':
            input_product = get('input')
            output_crs = recipe['reproject'].get('output_crs')
            resolution = recipe['reproject'].get('resolution')
            align = recipe['reproject'].get('align')

            self._assert(input_product is not None, "no input for reproject in {}".format(recipe))
            self._assert(output_crs is not None, "no output_crs for reproject in {}".format(recipe))
            self._assert(resolution is not None, "no resolution for reproject in {}".format(recipe))

            return from_validated_recipe(dict(reproject=recipe['reproject'],
                                              input=self.construct(**input_product),
                                              **reject_keys(recipe, ['reproject', 'input'])))

        raise VirtualProductException("could not understand virtual product recipe: {}".format(recipe))

    def register(self, namespace: str, name: str, callable_obj):
        """ Register a callable to the name resolver. """
        if namespace not in self.lookup_table:
            self.lookup_table[namespace] = {}

        if name in self.lookup_table[namespace]:
            raise VirtualProductException("name {} under {} is already registered".format(name, namespace))

        self.lookup_table[namespace][name] = callable_obj


DEFAULT_RESOLVER = NameResolver({'transform': dict(make_mask=MakeMask,
                                                   apply_mask=ApplyMask,
                                                   to_float=ToFloat,
                                                   rename=Rename,
                                                   select=Select,
                                                   expressions=Expressions),
                                 'aggregate': dict(xarray_reduction=XarrayReduction),
                                 'aggregate/group_by': dict(year=year,
                                                            month=month,
                                                            week=week,
                                                            day=day,
                                                            earliest_time=earliest_time)})


def construct(name_resolver=None, **recipe: Mapping[str, Any]) -> VirtualProduct:
    """
    Create a virtual product from a specification dictionary.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return name_resolver.construct(**recipe)


def construct_from_yaml(recipe: str, name_resolver=None) -> VirtualProduct:
    """
    Create a virtual product from a yaml recipe.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return construct(name_resolver=name_resolver, **parse_yaml(recipe))


def catalog_from_yaml(catalog_body: str, name_resolver=None) -> Catalog:
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
