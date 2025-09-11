# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import csv
import json
from collections.abc import Generator, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from gzip import GzipFile
from io import BytesIO
from types import SimpleNamespace

from botocore.client import BaseClient

from . import s3_client, s3_fetch, s3_ls_dir


def find_latest_manifest(prefix: str, s3: BaseClient | None, **kw) -> str:
    """
    Find latest manifest
    """
    manifest_dirs = sorted(s3_ls_dir(prefix, s3=s3, **kw), reverse=True)
    for d in manifest_dirs:
        if d.endswith("/"):
            leaf = d.split("/")[-2]
            if leaf.endswith("Z"):
                return d + "manifest.json"
    return ""


def retrieve_manifest_files(
    key: str, s3: BaseClient | None, schema: Iterable, **kw
) -> Generator[SimpleNamespace]:
    """
    Retrieve manifest file and return a namespace

    namespace(
        Bucket=<bucket_name>,
        Key=<key_path>,
        LastModifiedDate=<date>,
        Size=<size>
    )
    """
    bb = s3_fetch(key, s3=s3, **kw)
    gz = GzipFile(fileobj=BytesIO(bb), mode="r")
    csv_rdr = csv.reader(line.decode("utf8") for line in gz)
    for rec in csv_rdr:
        yield SimpleNamespace(**dict(zip(schema, rec)))


def list_inventory(
    manifest: str,
    s3: BaseClient | None = None,
    prefix: str = "",
    suffix: str = "",
    contains: str = "",
    n_threads: int | None = None,
    **kw,
) -> Generator[SimpleNamespace]:
    """
    Returns a generator of inventory records

    manifest -- s3:// url to manifest.json or a folder in which case latest one is chosen.

    :param manifest:
    :param s3:
    :param prefix:
    :param suffix:
    :param contains:
    :param n_threads: number of threads, if not sent does not use threads
    :return: SimpleNamespace
    """
    # TODO: refactor parallel execution part out of this function
    # pylint: disable=too-many-locals
    s3 = s3 or s3_client()

    if manifest.endswith("/"):
        manifest = find_latest_manifest(manifest, s3, **kw)

    info = json.loads(s3_fetch(manifest, s3=s3, **kw))

    must_have_keys = {"fileFormat", "fileSchema", "files", "destinationBucket"}
    missing_keys = must_have_keys - set(info)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    if info["fileFormat"].upper() != "CSV":
        raise ValueError("Data is not in CSV format")

    s3_prefix = "s3://" + info["destinationBucket"].split(":")[-1] + "/"
    data_urls = [s3_prefix + f["key"] for f in info["files"]]
    schema = tuple(info["fileSchema"].split(", "))

    if n_threads:
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            tasks = [
                executor.submit(retrieve_manifest_files, key, s3, schema)
                for key in data_urls
            ]

            for future in as_completed(tasks):
                for namespace in future.result():
                    key = namespace.Key
                    if (
                        key.startswith(prefix)
                        and key.endswith(suffix)
                        and contains in key
                    ):
                        yield namespace
    else:
        for u in data_urls:
            for namespace in retrieve_manifest_files(u, s3, schema):
                key = namespace.Key
                if key.startswith(prefix) and key.endswith(suffix) and contains in key:
                    yield namespace
