# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import itertools
import tarfile
import time
from collections.abc import Generator
from io import BytesIO
from pathlib import Path
from typing import IO


def tar_mode(
    gzip: bool | None = None, xz: bool | None = None, is_pipe: bool | None = None
) -> str:
    """Return tarfile.open compatible mode from boolean flags"""
    if gzip:
        return ":gz"
    if xz:
        return ":xz"
    if is_pipe:
        return "|"
    return ""


def tar_doc_stream(
    fname: str | Path | IO[bytes], mode: int | None = None, predicate=None
) -> Generator[tuple[str, bytes]]:
    """Read small documents (whole doc must fit into memory) from tar file.

    predicate : entry_info -> Bool
       return True for those entries that need to be read and False for those that need to be skipped.

       where `entry_info` is a tar entry info dictionary with keys like:
         name   -- internal path
         size   -- size in bytes
         mtime  -- timestamp as and integer

    mode: passed on to tarfile.open(..), things like 'r:gz'

    Function returns iterator of tuples (name:str, data:bytes)
    """
    if predicate:

        def should_skip(entry) -> bool:
            if not entry.isfile():
                return True
            return not predicate(entry.get_info())

    else:

        def should_skip(entry) -> bool:
            return not entry.isfile()

    def tar_open(fname: str | Path | IO[bytes], mode):
        if isinstance(fname, str | Path):
            open_args = [] if mode is None else [mode]
            return tarfile.open(fname, *open_args)

        return tarfile.open(mode=mode, fileobj=fname)

    with tar_open(fname, mode) as tar:
        for entry in itertools.filterfalse(should_skip, tar):
            with tar.extractfile(entry) as f:
                buf = f.read()
                yield entry.name, buf


def add_txt_file(
    tar: tarfile.TarFile,
    fname: str,
    content: str | bytes,
    mode: int = 0o644,
    last_modified: float | int | datetime.datetime | None = None,
) -> None:
    """Add file to tar from RAM (string or bytes) + name

    :param tar: tar file object opened for writing
    :param fname: path within tar file
    :param content: string or bytes, content or the file to write
    :param mode: file permissions octet
    :param last_modified: file modification timestamp
    """
    if last_modified is None:
        last_modified = time.time()

    if isinstance(last_modified, datetime.datetime):
        last_modified = last_modified.timestamp()

    info = tarfile.TarInfo(name=fname)
    if isinstance(content, str):
        content = content.encode("utf-8")
    info.size = len(content)
    info.mtime = last_modified
    info.mode = mode
    tar.addfile(tarinfo=info, fileobj=BytesIO(content))
