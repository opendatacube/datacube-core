from typing import Mapping, Any

from .impl import VirtualProduct, Transformation, VirtualProductException
from .impl import from_validated_recipe
from .transformations import MakeMask, ApplyMask, ToFloat, Rename, Select, Formula
from .transformations import Mean, year, month, week, day
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

            if not callable(result):
                raise VirtualProductException("{} not callable in {}".format(kind, recipe))

            return result

        kind_keys = {key for key in recipe if key in ['product', 'transform', 'collate', 'juxtapose', 'aggregate']}
        if len(kind_keys) < 1:
            raise VirtualProductException("virtual product kind not specified in {}".format(recipe))
        if len(kind_keys) > 1:
            raise VirtualProductException("ambiguous kind in {}".format(recipe))

        if 'product' in recipe:
            func_keys = ['fuse_func', 'dataset_predicate']
            return from_validated_recipe({key: value if key not in func_keys else lookup(value, kind='function')
                                          for key, value in recipe.items()})

        if 'transform' in recipe:
            cls_name = recipe['transform']
            input_product = get('input')

            if input_product is None:
                raise VirtualProductException("no input for transformation in {}".format(recipe))

            return from_validated_recipe(dict(transform=lookup(cls_name, 'transform'),
                                              input=self.construct(**input_product),
                                              **reject_keys(recipe, ['transform', 'input'])))

        if 'collate' in recipe:
            if len(recipe['collate']) < 1:
                raise VirtualProductException("no children for collate in {}".format(recipe))

            return from_validated_recipe(dict(collate=[self.construct(**child) for child in recipe['collate']],
                                              **reject_keys(recipe, ['collate'])))

        if 'juxtapose' in recipe:
            if len(recipe['juxtapose']) < 1:
                raise VirtualProductException("no children for juxtapose in {}".format(recipe))

            return from_validated_recipe(dict(juxtapose=[self.construct(**child) for child in recipe['juxtapose']],
                                              **reject_keys(recipe, ['juxtapose'])))

        if 'aggregate' in recipe:
            cls_name = recipe['aggregate']
            input_product = get('input')
            group_by = get('group_by')

            if input_product is None:
                raise VirtualProductException("no input for aggregate in {}".format(recipe))
            if group_by is None:
                raise VirtualProductException("no group_by for aggregate in {}".format(recipe))

            return from_validated_recipe(dict(aggregate=lookup(cls_name, 'aggregate'),
                                              group_by=lookup(group_by, 'aggregate/group_by', kind='group_by'),
                                              input=self.construct(**input_product),
                                              **reject_keys(recipe, ['aggregate', 'input', 'group_by'])))

        raise VirtualProductException("could not understand virtual product recipe: {}".format(recipe))


DEFAULT_RESOLVER = NameResolver({'transform': dict(make_mask=MakeMask,
                                                   apply_mask=ApplyMask,
                                                   to_float=ToFloat,
                                                   rename=Rename,
                                                   select=Select,
                                                   formula=Formula),
                                 'aggregate': dict(mean=Mean),
                                 'aggregate/group_by': dict(year=year,
                                                            month=month,
                                                            week=week,
                                                            day=day)})


def construct(name_resolver=None, **recipe: Mapping[str, Any]) -> VirtualProduct:
    """
    Create a virtual product from a specification dictionary.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return DEFAULT_RESOLVER.construct(**recipe)


def construct_from_yaml(recipe: str, name_resolver=None) -> VirtualProduct:
    """
    Create a virtual product from a yaml recipe.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return construct(**parse_yaml(recipe))


def catalog_from_yaml(catalog_body: str, name_resolver=None) -> Catalog:
    """
    Load a catalog of virtual products from a yaml document.
    """
    if name_resolver is None:
        name_resolver = DEFAULT_RESOLVER

    return Catalog(DEFAULT_RESOLVER, parse_yaml(catalog_body))
