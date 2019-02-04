from typing import Mapping, Any

from .impl import VirtualProduct, Transformation, VirtualProductException
from .transformations import MakeMask, ApplyMask, ToFloat, Rename, Select
from .utils import reject_keys

from datacube.model import Measurement
from datacube.utils import import_function
from datacube.utils.documents import parse_yaml

__all__ = ['construct', 'Transformation', 'Measurement']


class NameResolver:
    """ Apply a mapping from name to callable objects in a recipe. """

    def __init__(self, **lookup_table):
        self.lookup_table = lookup_table

    def construct(self, **recipe) -> VirtualProduct:
        """ Validate recipe and construct virtual product. """

        get = recipe.get

        kind_keys = {key for key in recipe if key in ['product', 'transform', 'collate', 'juxtapose']}
        if len(kind_keys) < 1:
            raise VirtualProductException("virtual product kind not specified in {}".format(recipe))
        elif len(kind_keys) > 1:
            raise VirtualProductException("ambiguous kind in {}".format(recipe))

        if 'product' in recipe:
            def resolve_func(key, value):
                if key not in ['fuse_func', 'dataset_predicate']:
                    return value

                if callable(value):
                    return value

                try:
                    return import_function(value)
                except (ImportError, AttributeError):
                    raise VirtualProductException("could not resolve function {} in {}".format(key, recipe))

            return VirtualProduct({key: resolve_func(key, value) for key, value in recipe.items()})

        if 'transform' in recipe:
            def resolve_transform(cls_name):
                if callable(cls_name):
                    return cls_name

                if cls_name in self.lookup_table:
                    cls = self.lookup_table[cls_name]
                else:
                    try:
                        cls = import_function(cls_name)
                    except (ImportError, AttributeError):
                        msg = "could not resolve transformation {} in {}".format(cls_name, recipe)
                        raise VirtualProductException(msg)

                if not callable(cls):
                    raise VirtualProductException("transformation not callable in {}".format(recipe))

                return cls

            cls_name = recipe['transform']
            input_product = get('input')

            if input_product is None:
                raise VirtualProductException("no input for transformation in {}".format(recipe))

            return VirtualProduct(dict(transform=resolve_transform(cls_name), input=self.construct(**input_product),
                                       **reject_keys(recipe, ['transform', 'input'])))

        if 'collate' in recipe:
            if len(recipe['collate']) < 1:
                raise VirtualProductException("no children for collate in {}".format(recipe))

            return VirtualProduct(dict(collate=[self.construct(**child) for child in recipe['collate']],
                                       **reject_keys(recipe, ['collate'])))

        if 'juxtapose' in recipe:
            if len(recipe['juxtapose']) < 1:
                raise VirtualProductException("no children for juxtapose in {}".format(recipe))

            return VirtualProduct(dict(juxtapose=[self.construct(**child) for child in recipe['juxtapose']],
                                       **reject_keys(recipe, ['juxtapose'])))

        raise VirtualProductException("could not understand virtual product recipe: {}".format(recipe))


DEFAULT_RESOLVER = NameResolver(make_mask=MakeMask,
                                apply_mask=ApplyMask,
                                to_float=ToFloat,
                                rename=Rename,
                                select=Select)


def construct(**recipe: Mapping[str, Any]) -> VirtualProduct:
    """
    Create a virtual product from a specification dictionary.
    """
    return DEFAULT_RESOLVER.construct(**recipe)


def construct_from_yaml(recipe: str) -> VirtualProduct:
    """
    Create a virtual product from a yaml recipe.
    """
    return construct(**parse_yaml(recipe))
