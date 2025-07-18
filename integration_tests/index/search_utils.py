# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import csv
import io
from collections.abc import Iterable

import datacube.scripts.search_tool
from datacube.model import Dataset, Product


def _load_product_query(
    lazy_results: Iterable[tuple[Product, Iterable[Dataset]]],
) -> dict[str, list[Dataset]]:
    """
    search_by_product() returns two levels of laziness. load them all into memory
    for easy comparison/counts
    """
    products = {}  # type: Dict[str, List[Dataset]]
    for product, datasets in lazy_results:
        assert product.name not in products, (
            "search_by_product() returned a product twice"
        )
        products[product.name] = list(datasets)
    return products


def _csv_search_raw(args, clirunner):
    # Do a CSV search from the cli, returning output as a string
    result = clirunner(
        ["-f", "csv"] + list(args),
        cli_method=datacube.scripts.search_tool.cli,
        verbose_flag=False,
    )
    output = result.output
    output_lines = output.split("\n")
    return "\n".join(line for line in output_lines if "WARNING" not in line)


def _cli_csv_search(args, clirunner):
    # Do a CSV search from the cli, returning results as a list of dictionaries
    output = _csv_search_raw(args, clirunner)
    return list(csv.DictReader(io.StringIO(output)))
