# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
High level indexing operations/utilities
"""
import logging

import json
import toolz
from uuid import UUID
from typing import cast, Any, Callable, Optional, Iterable, List, Mapping, Sequence, Tuple, Union, MutableMapping

from datacube.model import Dataset, LineageTree, Product
from datacube.index.abstract import AbstractIndex
from datacube.utils import changes, InvalidDocException, SimpleDocNav, jsonify_document
from datacube.model.utils import BadMatch, dedup_lineage, remap_lineage_doc, flatten_datasets
from datacube.utils.changes import get_doc_changes
from datacube.model import LineageDirection
from .eo3 import prep_eo3, is_doc_eo3, is_doc_geo  # type: ignore[attr-defined]

_LOG = logging.getLogger(__name__)


class ProductRule:
    def __init__(self, product: Product, signature: Mapping[str, Any]):
        self.product = product
        self.signature = signature


def load_rules_from_types(index: AbstractIndex,
                          product_names: Optional[Iterable[str]] = None,
                          excluding: Optional[Iterable[str]] = None
                          ) -> Union[Tuple[List[ProductRule], None], Tuple[None, str]]:
    products: List[Product] = []
    if product_names:
        for name in product_names:
            product = index.products.get_by_name(name)
            if not product:
                return None, 'Supplied product name "%s" not present in the database' % name
            products.append(product)
    else:
        products += index.products.get_all()

    if excluding is not None:
        excluding = set(excluding)
        products = [p for p in products if p.name not in excluding]

    if len(products) == 0:
        return None, 'Found no matching products in the database'

    return [ProductRule(p, p.metadata_doc) for p in products], None


ProductMatcher = Callable[[Mapping[str, Any]], Product]


def product_matcher(rules: Sequence[ProductRule]) -> ProductMatcher:
    """Given product matching rules return a function mapping a document to a
    matching product.

    """
    assert len(rules) > 0

    def matches(doc: Mapping[str, Any], rule: ProductRule) -> bool:
        return changes.contains(doc, rule.signature)

    def single_product_matcher(rule):
        def match(doc: Mapping[str, Any]) -> bool:
            if matches(doc, rule):
                return rule.product

            raise BadMatch('Dataset metadata did not match product signature.'
                           '\nDataset definition:\n %s\n'
                           '\nProduct signature:\n %s\n'
                           % (json.dumps(doc, indent=4),
                              json.dumps(rule.signature, indent=4)))

        return match

    if len(rules) == 1:
        return single_product_matcher(rules[0])

    def match(doc: Mapping[str, Any]) -> Product:
        matched = [rule.product for rule in rules if changes.contains(doc, rule.signature)]

        if len(matched) == 1:
            return matched[0]

        doc_id = doc.get('id', '<missing id>')

        if len(matched) == 0:
            raise BadMatch('No matching Product found for dataset %s' % doc_id)
        else:
            raise BadMatch('Auto match failed, dataset %s matches several products:\n  %s' % (
                doc_id,
                ','.join(p.name for p in matched)))

    return match


def check_dataset_consistent(dataset: Dataset) -> Tuple[bool, Optional[str]]:
    """
    :type dataset: datacube.model.Dataset
    :return: (Is consistent, [error message|None])
    :rtype: (bool, str or None)
    """
    product_measurements = set(dataset.product.measurements.keys())

    if len(product_measurements) == 0:
        return True, None

    if dataset.measurements is None:
        return False, "No measurements defined for a dataset"

    # It the type expects measurements, ensure our dataset contains them all.
    if not product_measurements.issubset(dataset.measurements.keys()):
        # Exclude 3D measurements since it's just a mapping to 2D measurements
        not_measured = {
            m
            for m in product_measurements - set(dataset.measurements.keys())
            if "extra_dim" not in dataset.product.measurements.get(m, [])
        }

        if not_measured:
            msg = "The dataset is not specifying all of the measurements in this product.\n"
            msg += "Missing fields are;\n" + str(not_measured)
            return False, msg

    return True, None


def check_consistent(a: Mapping[str, Any], b: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
    diffs = get_doc_changes(a, b)
    if len(diffs) == 0:
        return True, None

    def render_diff(offset, a, b):
        offset = '.'.join(map(str, offset))
        return '{}: {!r}!={!r}'.format(offset, a, b)

    return False, ", ".join([render_diff(offset, a, b) for offset, a, b in diffs])


DatasetOrError = Union[
    Tuple[Dataset, None],
    Tuple[None, Union[str, Exception]]
]


def check_intended_eo3(ds: SimpleDocNav, product: Product) -> None:
    # warn if it looks like dataset was meant to be eo3 but is not
    if not is_doc_eo3(ds.doc) and ("eo3" in product.metadata_type.name):
        _LOG.warning(f"Dataset {ds.id} has a product with an eo3 metadata type, "
                     "but the dataset definition does not include the $schema field "
                     "and so will not be recognised as an eo3 dataset.")


def resolve_no_lineage(ds: SimpleDocNav,
                       uri: str,
                       matcher: ProductMatcher,
                       source_tree: Optional[LineageTree] = None) -> DatasetOrError:
    if source_tree:
        raise ValueError("source_tree passed to non-lineage resolver")
    doc = ds.doc_without_lineage_sources
    try:
        product = matcher(doc)
    except BadMatch as e:
        return None, e
    check_intended_eo3(ds, product)
    return Dataset(product, doc, uris=[uri], sources={}), None


def resolve_with_lineage(doc: SimpleDocNav, uri: str, matcher: ProductMatcher,
                         source_tree: Optional[LineageTree] = None,
                         home_index: Optional[str] = None) -> DatasetOrError:
    """
    Dataset driver for the (new) external lineage API

    API paramters
    :param doc: Dataset docnav
    :param uri: location uri

    Extra kwargs passed in by Doc2Dataset:
    :param matcher: Product matcher
    :param source_tree: sourcewards LineageTree to use in place of EO3 sources (optional)
    :param home_index: Home for sources (ignored if source_tree is not none)
    :return:
    """
    uuid_ = doc.id
    if not uuid_:
        return None, "No id defined in dataset doc"
    try:
        product = matcher(doc.doc)
    except BadMatch as e:
        return None, e
    if source_tree is None:
        # Get sources from EO3 document, use home_index as home of source id's
        source_tree = LineageTree.from_data(uuid_, sources=doc.sources, home=home_index)
    else:
        # May be None
        if source_tree.direction == LineageDirection.DERIVED:
            raise ValueError("source_tree cannot be a derived tree.")
        source_tree = source_tree.find_subtree(uuid_)
    check_intended_eo3(doc, product)
    return Dataset(product,
                   doc.doc,
                   source_tree=source_tree,
                   uris=[uri]), None


def resolve_legacy_lineage(main_ds_doc: SimpleDocNav, uri: str, matcher: ProductMatcher,
                           index: AbstractIndex,
                           fail_on_missing_lineage: bool,
                           verify_lineage: bool,
                           source_tree: Optional[LineageTree] = None,
                           ) -> DatasetOrError:
    if source_tree:
        raise ValueError("source_tree passed to non-external lineage resolver")

    try:
        main_ds = SimpleDocNav(dedup_lineage(main_ds_doc))
    except InvalidDocException as e:
        return None, e

    main_uuid = main_ds.id
    if not main_uuid:
        return None, "No id defined in dataset doc"

    ds_by_uuid = toolz.valmap(toolz.first, flatten_datasets(main_ds))
    all_uuid = list(ds_by_uuid)
    db_dss = {ds.id: ds for ds in index.datasets.bulk_get(all_uuid)}

    lineage_uuids = set(filter(lambda x: x != main_uuid, all_uuid))
    missing_lineage = lineage_uuids - set(db_dss)

    if missing_lineage and fail_on_missing_lineage:
        return None, "Following lineage datasets are missing from DB: %s" % (
            ','.join(str(m) for m in missing_lineage))

    if not is_doc_eo3(main_ds.doc):
        if is_doc_geo(main_ds.doc, check_eo3=False):
            if not index.supports_legacy:
                return None, "Legacy metadata formats not supported by the current index driver."
        else:
            if not index.supports_nongeo:
                return None, "Non-geospatial metadata formats not supported by the current index driver."
        if verify_lineage:
            bad_lineage = []

            for uuid in lineage_uuids:
                if uuid in db_dss:
                    ok, err = check_consistent(jsonify_document(ds_by_uuid[uuid].doc_without_lineage_sources),
                                               db_dss[uuid].metadata_doc)
                    if not ok:
                        bad_lineage.append((uuid, err))

            if len(bad_lineage) > 0:
                error_report = '\n'.join('Inconsistent lineage dataset {}:\n> {}'.format(uuid, err)
                                         for uuid, err in bad_lineage)
                return None, error_report

    def with_cache(v: Dataset, k: UUID, cache: MutableMapping[UUID, Dataset]) -> Dataset:
        cache[k] = v
        return v

    def resolve_ds(ds: SimpleDocNav,
                   sources: Optional[Mapping[UUID, Dataset]],
                   cache: MutableMapping[UUID, Dataset]) -> Dataset:
        cached = cache.get(ds.id)
        if cached is not None:
            return cached

        uris = [uri] if ds.id == main_uuid else []

        doc = ds.doc

        db_ds = db_dss.get(ds.id)
        if db_ds:
            product = db_ds.product
        else:
            product = matcher(doc)

        check_intended_eo3(ds, product)
        return with_cache(Dataset(product, doc, uris=uris, sources=sources), ds.id, cache)
    try:
        return remap_lineage_doc(main_ds, resolve_ds, cache={}), None
    except BadMatch as e:
        return None, e


def dataset_resolver(index: AbstractIndex,
                     match_product: Callable[[Mapping[str, Any]], Product],
                     fail_on_missing_lineage: bool = False,
                     verify_lineage: bool = True,
                     skip_lineage: bool = False,
                     home_index: Optional[str] = None) -> Callable[
    [SimpleDocNav, str, Optional[LineageTree]],
    DatasetOrError
]:
    if skip_lineage or not index.supports_lineage:
        # Resolver that ignores lineage.
        resolver = resolve_no_lineage
        extra_kwargs = {
            "matcher": match_product,
        }
    elif index.supports_external_lineage:
        # ODCv2 external lineage API resolver
        resolver = resolve_with_lineage
        extra_kwargs = {
            "matcher": match_product,
            "home_index": home_index,
        }
    else:
        # Legacy lineage API resolver
        resolver = resolve_legacy_lineage
        extra_kwargs = {
            "matcher": match_product,
            "index": index,
            "fail_on_missing_lineage": fail_on_missing_lineage,
            "verify_lineage": verify_lineage,
        }

    def resolve(doc: SimpleDocNav, uri: str, source_tree: Optional[LineageTree] = None) -> DatasetOrError:
        return resolver(doc, uri, source_tree=source_tree, **extra_kwargs)

    return resolve


class Doc2Dataset:
    """Used for constructing `Dataset` objects from plain metadata documents.

    This requires a database connection to perform the automatic matching against
    available products.

    There are options for including and excluding the products to match against,
    as well as how to deal with source lineage.

    Once constructed, call with a dictionary object and location URI, eg::

        resolver = Doc2Dataset(index)
        dataset = resolver(dataset_dictionary, 'file:///tmp/test-dataset.json')
        index.dataset.add(dataset)


    :param index: an open Database connection

    :param list products: List of product names against which to match datasets
                          (including lineage datasets). If not supplied we will
                          consider all products.

    :param list exclude_products: List of products to exclude from matching

    :param fail_on_missing_lineage: If True fail resolve if any lineage
                                    datasets are missing from the DB

                                    Only False supported if index.supports_external_lineage is True.

    :param verify_lineage: If True check that lineage datasets in the
                           supplied document are identical to DB versions

                           Ignored for EO3 documents.  Will be dropped in ODCv2 as only eo3 documents
                           will be supported.

    :param skip_lineage: If True ignore lineage sub-tree in the supplied
                         document and construct dataset without lineage datasets
    :param eo3: 'auto'/True/False by default auto-detect EO3 datasets and pre-process them

                Cannot be 'False' if index.supports_legacy is False.
                Will be dropped in ODCv2 as only eo3 documents will be supported
    :param home_index: Ignored if index.supports_exernal_home is False.  Defaults to None.
                Optional string labelling the "home index" for lineage datasets.
                home_index is ignored if an explicit source_tree is passed to the resolver.
    """
    def __init__(self,
                 index: AbstractIndex,
                 products: Optional[Sequence[str]] = None,
                 exclude_products: Optional[Sequence[str]] = None,
                 fail_on_missing_lineage: bool = False,
                 verify_lineage: bool = True,
                 skip_lineage: bool = False,
                 eo3: Union[bool, str] = 'auto',
                 home_index: Optional[str] = None):
        if not index.supports_lineage:
            skip_lineage = True
            verify_lineage = False
            fail_on_missing_lineage = False
            home_index = None
        else:
            if not index.supports_legacy and not index.supports_nongeo:
                if not eo3:
                    raise ValueError("EO3 cannot be set to False for a non-legacy geo-only index.")
                eo3 = True
            if index.supports_external_lineage and fail_on_missing_lineage:
                raise ValueError("fail_on_missing_lineage is not supported for this index driver.")
            if home_index and skip_lineage:
                raise ValueError("Cannot provide a default home_index when skip_lineage is set.")

        rules, err_msg = load_rules_from_types(index,
                                               product_names=products,
                                               excluding=exclude_products)
        if rules is None:
            raise ValueError(err_msg)

        self.index = index
        self._eo3 = eo3
        matcher = product_matcher(rules)
        self._ds_resolve = dataset_resolver(index,
                                            matcher,
                                            fail_on_missing_lineage=fail_on_missing_lineage,
                                            verify_lineage=verify_lineage,
                                            skip_lineage=skip_lineage,
                                            home_index=home_index)

    def __call__(self, doc_in: Union[SimpleDocNav, Mapping[str, Any]], uri: str,
                 source_tree: Optional[LineageTree] = None) -> DatasetOrError:
        """Attempt to construct dataset from metadata document and a uri.

        :param doc: Dictionary or SimpleDocNav object
        :param uri: String "location" property of the Dataset

        :return: (dataset, None) is successful,
        :return: (None, ErrorMessage) on failure
        """
        if isinstance(doc_in, SimpleDocNav):
            doc: SimpleDocNav = doc_in
        else:
            doc = SimpleDocNav(doc_in)

        if self._eo3:
            auto_skip = self._eo3 == 'auto'
            doc = SimpleDocNav(
                prep_eo3(
                    doc.doc,
                    auto_skip=auto_skip,
                    remap_lineage=not self.index.supports_external_lineage
                )
            )

        dataset, err = self._ds_resolve(doc, uri, source_tree=source_tree)
        if dataset is None:
            return None, cast(Union[str, Exception], err)

        is_consistent, reason = check_dataset_consistent(dataset)
        if not is_consistent:
            return None, cast(Union[str, Exception], reason)

        return dataset, None
