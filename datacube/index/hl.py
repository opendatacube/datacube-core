# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
High level indexing operations/utilities
"""
import json
import toolz
from uuid import UUID
from typing import cast, Any, Callable, Optional, Iterable, List, Mapping, Sequence, Tuple, Union, MutableMapping

from datacube.model import Dataset, DatasetType as Product
from datacube.index.abstract import AbstractIndex
from datacube.utils import changes, InvalidDocException, SimpleDocNav, jsonify_document
from datacube.model.utils import BadMatch, dedup_lineage, remap_lineage_doc, flatten_datasets
from datacube.utils.changes import get_doc_changes
from .eo3 import prep_eo3, is_doc_eo3, is_doc_geo  # type: ignore[attr-defined]


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


def product_matcher(rules: Sequence[ProductRule]) -> Callable[[Mapping[str, Any]], Product]:
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

            relevant_doc = {k: v for k, v in doc.items() if k in rule.signature}
            raise BadMatch('Dataset metadata did not match product signature.'
                           '\nDataset definition:\n %s\n'
                           '\nProduct signature:\n %s\n'
                           % (json.dumps(relevant_doc, indent=4),
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
    product_measurements = set(dataset.type.measurements.keys())

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
            if "extra_dim" not in dataset.type.measurements.get(m, [])
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


def dataset_resolver(index: AbstractIndex,
                     product_matching_rules: Sequence[ProductRule],
                     fail_on_missing_lineage: bool = False,
                     verify_lineage: bool = True,
                     skip_lineage: bool = False) -> Callable[[SimpleDocNav, str], DatasetOrError]:
    match_product = product_matcher(product_matching_rules)

    def resolve_no_lineage(ds: SimpleDocNav, uri: str) -> DatasetOrError:
        doc = ds.doc_without_lineage_sources
        try:
            product = match_product(doc)
        except BadMatch as e:
            return None, e

        return Dataset(product, doc, uris=[uri], sources={}), None

    def resolve(main_ds_doc: SimpleDocNav, uri: str) -> DatasetOrError:
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
                product = db_ds.type
            else:
                product = match_product(doc)

            return with_cache(Dataset(product, doc, uris=uris, sources=sources), ds.id, cache)
        try:
            return remap_lineage_doc(main_ds, resolve_ds, cache={}), None
        except BadMatch as e:
            return None, e
    return resolve_no_lineage if skip_lineage else resolve


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

    :param verify_lineage: If True check that lineage datasets in the
                           supplied document are identical to DB versions

    :param skip_lineage: If True ignore lineage sub-tree in the supplied
                         document and construct dataset without lineage datasets
    :param eo3: 'auto'/True/False by default auto-detect EO3 datasets and pre-process them
    """
    def __init__(self,
                 index: AbstractIndex,
                 products: Optional[Sequence[str]] = None,
                 exclude_products: Optional[Sequence[str]] = None,
                 fail_on_missing_lineage: bool = False,
                 verify_lineage: bool = True,
                 skip_lineage: bool = False,
                 eo3: Union[bool, str] = 'auto'):
        if not index.supports_legacy and not index.supports_nongeo:
            if not eo3:
                raise ValueError("EO3 cannot be set to False for a non-legacy geo-only index.")
            eo3 = True
        rules, err_msg = load_rules_from_types(index,
                                               product_names=products,
                                               excluding=exclude_products)
        if rules is None:
            raise ValueError(err_msg)

        self._eo3 = eo3
        self._ds_resolve = dataset_resolver(index,
                                            rules,
                                            fail_on_missing_lineage=fail_on_missing_lineage,
                                            verify_lineage=verify_lineage,
                                            skip_lineage=skip_lineage)

    def __call__(self, doc_in: Union[SimpleDocNav, Mapping[str, Any]], uri: str) -> DatasetOrError:
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
            doc = SimpleDocNav(prep_eo3(doc.doc, auto_skip=auto_skip))

        dataset, err = self._ds_resolve(doc, uri)
        if dataset is None:
            return None, cast(Union[str, Exception], err)

        is_consistent, reason = check_dataset_consistent(dataset)
        if not is_consistent:
            return None, cast(Union[str, Exception], reason)

        return dataset, None
