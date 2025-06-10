# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Query datasets.
"""

import csv
import datetime
import shutil
import sys
from collections.abc import Callable, Collection
from functools import partial, singledispatch
from os import terminal_size as t_size
from typing import Any

import click
from sqlalchemy.dialects.postgresql import Range

from datacube.ui import click as ui
from datacube.ui.click import CLICK_SETTINGS
from datacube.utils.dates import tz_as_utc

PASS_INDEX = ui.pass_index("datacube-search")


def printable_values(d) -> dict:
    return {k: printable(v) for k, v in d.items()}


def write_pretty(
    out_f,
    field_names: Collection[str],
    search_results,
    terminal_size: t_size = shutil.get_terminal_size(),
) -> None:
    """
    Output in a human-readable text format. Inspired by psql's expanded output.
    """
    terminal_width = terminal_size[0]
    record_num = 1

    field_header_width = max(len(name) for name in field_names)
    field_output_format = "{:<" + str(field_header_width) + "} | {}"

    for result in search_results:
        separator_line = f"-[ {record_num} ]"
        separator_line += "-" * (terminal_width - len(separator_line) - 1)
        click.echo(separator_line, file=out_f)

        for name, value in sorted(result.items()):
            click.echo(field_output_format.format(name, printable(value)), file=out_f)

        record_num += 1


def write_csv(out_f: Any, field_names: Collection, search_results: dict) -> None:
    """
    Output as a CSV.
    """
    search_results = list(search_results)
    writer = csv.DictWriter(out_f, tuple(sorted(field_names)))
    writer.writeheader()
    writer.writerows(printable_values(d) for d in search_results)


OUTPUT_FORMATS: dict[str, Callable[[Any, Collection, dict], None]] = {
    "csv": write_csv,
    "pretty": write_pretty,
}


@click.group(help="Search the Data Cube", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
@click.option(
    "-f",
    type=click.Choice(list(OUTPUT_FORMATS)),
    default="pretty",
    show_default=True,
    help="Output format",
)
@click.pass_context
def cli(ctx, f) -> None:
    ctx.obj["write_results"] = partial(OUTPUT_FORMATS[f], sys.stdout)


@cli.command()
@ui.parsed_search_expressions
@PASS_INDEX
@click.pass_context
def datasets(ctx, index, expressions) -> None:
    """
    Search available Datasets
    """
    ctx.obj["write_results"](
        sorted(index.products.get_field_names()),
        (tup._asdict() for tup in index.datasets.search_returning(**expressions)),
    )


@cli.command("product-counts")
@click.argument("period", nargs=1)
@ui.parsed_search_expressions
@PASS_INDEX
def product_counts(index, period, expressions) -> None:
    """
    Count product Datasets available by period

    PERIOD: eg. 1 month, 6 months, 1 year
    """
    for product, series in index.datasets.count_by_product_through_time(
        period, **expressions
    ):
        click.echo(product.name)
        for timerange, count in series:
            formatted_dt = tz_as_utc(timerange[0]).strftime("%Y-%m-%d")
            click.echo(f"    {formatted_dt}: {count}")


@singledispatch
def printable(val):
    return val


@printable.register(type(None))
def printable_none(val) -> str:
    return ""


@printable.register(datetime.datetime)
def printable_dt(val: datetime.datetime) -> str:
    return tz_as_utc(val).isoformat()


@printable.register(Range)
def printable_r(val: Range) -> str:
    if val.lower_inf:
        return printable(val.upper)
    if val.upper_inf:
        return printable(val.lower)

    return f"{printable(val.lower)} to {printable(val.upper)}"


if __name__ == "__main__":
    cli()
