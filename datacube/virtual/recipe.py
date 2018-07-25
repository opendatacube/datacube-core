"""
Utility to convert a virtual product specification (a recipe) into a usable
virtual product object.
"""

from datacube.utils import import_function
from datacube.virtual.impl import VirtualProductException, Transformation
from datacube.virtual.impl import Collate, Juxtapose, Transform, BasicProduct

import yaml

def create(recipe):
    # type: (Dict[str, Any]) -> VirtualProduct
    """
    Create a virtual product from a specification.
    """
    get = recipe.get

    if 'collate' in recipe:
        return Collate(*[create(child) for child in get('collate', [])],
                       index_measurement_name=get('index_measurement_name'))

    if 'juxtapose' in recipe:
        return Juxtapose(*[create(child) for child in get('juxtapose', [])])

    if 'transform' in recipe:
        child = get('source')
        cls = get('transform')

        if cls is None:
            raise VirtualProductException("no transformation provided in {}".format(recipe))

        if child is None:
            raise VirtualProductException("no source for transformation in {}".format(recipe))

        if not callable(cls):
            if isinstance(cls, str):
                cls = import_function(cls)
            else:
                raise VirtualProductException("not a transformation class: {}".format(cls))

        obj = cls(**{key: value for key, value in recipe.items() if key not in ['transform', 'source']})
        if not isinstance(obj, Transformation):
            raise VirtualProductException("not a transformation object: {}".format(obj))

        return Transform(create(child), obj)

    if 'product' in recipe:
        def resolve_func(key, value):
            if key not in ['fuse_func', 'dataset_predicate']:
                return value

            if callable(value):
                return value

            return import_function(value)

        return BasicProduct(**{key: resolve_func(key, value)
                               for key, value in recipe.items()})

    raise VirtualProductException("could not understand virtual product recipe: {}".format(recipe))


def qualified_name(func):
    return func.__module__ + '.' + func.__qualname__


def reconstruct(product):
    # type: (VirtualProduct) -> Dict[str, Any]
    """
    Attempt to reconstruct recipe from the virtual product (useful for debugging).
    Recreating the product from this recipe is not guaranteed to succeed.
    """
    def specified(**settings):
        """ Only keep the settings that has been specified, ignoring `None`s.  """
        return {key: value
                for key, value in settings.items()
                if value is not None}

    if isinstance(product, Collate):
        return specified(collate=[reconstruct(child) for child in product.children],
                         index_measurement_name=product.index_measurement_name)

    if isinstance(product, Juxtapose):
        return {'juxtapose': [reconstruct(child) for child in product.children]}

    if isinstance(product, Transform):
        source = reconstruct(product.source)
        args = specified(**vars(product.transformation))
        transformation = qualified_name(type(product.transformation))
        return specified(source=source, transform=transformation, **args)

    if isinstance(product, BasicProduct):
        def unresolve_func(key, value):
            if key not in ['fuse_func', 'dataset_predicate']:
                return value

            return qualified_name(value)

        return specified(product=product.product, **{key: unresolve_func(key, value)
                                                     for key, value in product.settings.items()})


def show(product, default_flow_style=False, indent=2):
    """
    Show the recipe for the datacube as a `yaml` document (useful for debugging).
    Recreating the product from this recipe is not guaranteed to succeed.
    """
    # NOTE: this could be nicer with sort_keys=False
    # but unfortunately that PR has not been merged yet
    return yaml.dump(reconstruct(product), Dumper=yaml.CDumper,
                     default_flow_style=default_flow_style, indent=indent)
