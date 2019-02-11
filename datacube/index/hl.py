"""
High level indexing operations/utilities
"""
import json
import toolz
from types import SimpleNamespace

from datacube.model import Dataset
from datacube.utils import changes, InvalidDocException, SimpleDocNav, jsonify_document
from datacube.model.utils import dedup_lineage, remap_lineage_doc, flatten_datasets
from datacube.utils.changes import get_doc_changes


class BadMatch(Exception):
    pass


def load_rules_from_types(index, product_names=None, excluding=None):
    products = []
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
        return None, 'Found no products in the database'

    return [SimpleNamespace(product=p, signature=p.metadata_doc) for p in products], None


def product_matcher(rules):
    """Given product matching rules return a function mapping a document to a
    matching product.

    """
    assert len(rules) > 0

    def matches(doc, rule):
        return changes.contains(doc, rule.signature)

    def single_product_matcher(rule):
        def match(doc):
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

    def match(doc):
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


def check_dataset_consistent(dataset):
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
        not_measured = str(product_measurements - set(dataset.measurements.keys()))
        msg = "The dataset is not specifying all of the measurements in this product.\n"
        msg += "Missing fields are;\n" + not_measured
        return False, msg

    return True, None


def check_consistent(a, b):
    diffs = get_doc_changes(a, b)
    if len(diffs) == 0:
        return True, None

    def render_diff(offset, a, b):
        offset = '.'.join(map(str, offset))
        return '{}: {!r}!={!r}'.format(offset, a, b)

    return False, ", ".join([render_diff(offset, a, b) for offset, a, b in diffs])


def dataset_resolver(index,
                     product_matching_rules,
                     fail_on_missing_lineage=False,
                     verify_lineage=True,
                     skip_lineage=False):
    match_product = product_matcher(product_matching_rules)

    def resolve_no_lineage(ds, uri):
        doc = ds.doc_without_lineage_sources
        try:
            product = match_product(doc)
        except BadMatch as e:
            return None, e

        return Dataset(product, doc, uris=[uri], sources={}), None

    def resolve(main_ds, uri):
        try:
            main_ds = SimpleDocNav(dedup_lineage(main_ds))
        except InvalidDocException as e:
            return None, e

        main_uuid = main_ds.id

        ds_by_uuid = toolz.valmap(toolz.first, flatten_datasets(main_ds))
        all_uuid = list(ds_by_uuid)
        db_dss = {str(ds.id): ds for ds in index.datasets.bulk_get(all_uuid)}

        lineage_uuids = set(filter(lambda x: x != main_uuid, all_uuid))
        missing_lineage = lineage_uuids - set(db_dss)

        if missing_lineage and fail_on_missing_lineage:
            return None, "Following lineage datasets are missing from DB: %s" % (','.join(missing_lineage))

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

        def with_cache(v, k, cache):
            cache[k] = v
            return v

        def resolve_ds(ds, sources, cache=None):
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


class Doc2Dataset(object):
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
                          (including lineage datasets). if not supplied we will
                          consider all products.

    :param list exclude_products: List of products to exclude from matching

    :param fail_on_missing_lineage: If True fail resolve if any lineage
                                    datasets are missing from the DB

    :param verify_lineage: If True check that lineage datasets in the
                           supplied document are identical to DB versions

    :param skip_lineage: If True ignore lineage sub-tree in the supplied
                         document and construct dataset without lineage datasets

    :param index: Database

    :param products: List of product names against which to match datasets
                     (including lineage datasets), if not supplied will consider all
                     products.

    :param exclude_products: List of products to exclude from matching

    :param fail_on_missing_lineage: If True fail resolve if any lineage
                                    datasets are missing from the DB

    :param verify_lineage: If True check that lineage datasets in the
                           supplied document are identical to DB versions

    :param skip_lineage: If True ignore lineage sub-tree in the supplied
                         document and construct dataset without lineage datasets

    """
    def __init__(self,
                 index,
                 products=None,
                 exclude_products=None,
                 fail_on_missing_lineage=False,
                 verify_lineage=True,
                 skip_lineage=False):
        rules, err_msg = load_rules_from_types(index,
                                               product_names=products,
                                               excluding=exclude_products)
        if rules is None:
            raise ValueError(err_msg)

        self._ds_resolve = dataset_resolver(index,
                                            rules,
                                            fail_on_missing_lineage=fail_on_missing_lineage,
                                            verify_lineage=verify_lineage,
                                            skip_lineage=skip_lineage)

    def __call__(self, doc, uri):
        """Attempt to construct dataset from metadata document and a uri.

        :param doc: Dictionary or SimpleDocNav object
        :param uri: String "location" property of the Dataset

        :return: (dataset, None) is successful,
        :return: (None, ErrorMessage) on failure
        """
        if not isinstance(doc, SimpleDocNav):
            doc = SimpleDocNav(doc)

        dataset, err = self._ds_resolve(doc, uri)
        if dataset is None:
            return None, err

        is_consistent, reason = check_dataset_consistent(dataset)
        if not is_consistent:
            return None, reason

        return dataset, None
