# coding=utf-8
"""
Common methods for UI code.
"""
from pathlib import Path
from typing import Union

from toolz.functoolz import identity

from datacube.utils import read_documents, InvalidDocException, SimpleDocNav, is_supported_document_type, is_url


def get_metadata_path(possible_path: Union[str, Path]):
    """
    Find a metadata path for a given input/dataset path.

    Needs to handle local files as well as remote URLs

    :rtype: str
    """
    # We require exact URLs, lets skip any sort of fancy investigation and mapping
    if is_url(possible_path):
        return possible_path

    dataset_path = Path(possible_path)

    # They may have given us a metadata file directly.
    if dataset_path.is_file() and is_supported_document_type(dataset_path):
        return dataset_path

    # Otherwise there may be a sibling file with appended suffix '.agdc-md.yaml'.
    expected_name = dataset_path.parent.joinpath('{}.agdc-md'.format(dataset_path.name))
    found = _find_any_metadata_suffix(expected_name)
    if found:
        return found

    # Otherwise if it's a directory, there may be an 'agdc-metadata.yaml' file describing all contained datasets.
    if dataset_path.is_dir():
        expected_name = dataset_path.joinpath('agdc-metadata')
        found = _find_any_metadata_suffix(expected_name)
        if found:
            return found

    raise ValueError('No metadata found for input %r' % dataset_path)


def _find_any_metadata_suffix(path):
    """
    Find any supported metadata files that exist with the given file path stem.
    (supported suffixes are tried on the name)

    Eg. searching for '/tmp/ga-metadata' will find if any files such as '/tmp/ga-metadata.yaml' or
    '/tmp/ga-metadata.json', or '/tmp/ga-metadata.yaml.gz' etc that exist: any suffix supported by read_documents()

    :type path: pathlib.Path
    """
    existing_paths = list(filter(is_supported_document_type, path.parent.glob(path.name + '*')))
    if not existing_paths:
        return None

    if len(existing_paths) > 1:
        raise ValueError('Multiple matched metadata files: {!r}'.format(existing_paths))

    return existing_paths[0]


def ui_path_doc_stream(paths, logger=None, uri=True, raw=False):
    """Given a stream of URLs, or Paths that could be directories, generate a stream of
    (path, doc) tuples.

    For every path:
    1. If directory find the metadata file or log error if not found

    2. Load all documents from that path and return one at a time (parsing
    errors are logged, but processing should continue)

    :param paths: Filesystem paths

    :param logger: Logger to use to report errors

    :param uri: If True return path in uri format, else return it as filesystem path

    :param raw: By default docs are wrapped in :class:`SimpleDocNav`, but you can
    instead request them to be raw dictionaries

    """

    def on_error1(p, e):
        if logger is not None:
            logger.error('No supported metadata docs found for dataset %s', str(p))

    def on_error2(p, e):
        if logger is not None:
            logger.error('Failed reading documents from %s', str(p))

    yield from _path_doc_stream(_resolve_doc_files(paths, on_error=on_error1),
                                on_error=on_error2, uri=uri, raw=raw)


def _resolve_doc_files(paths, on_error):
    for p in paths:
        try:
            yield get_metadata_path(p)
        except ValueError as e:
            on_error(p, e)


def _path_doc_stream(files, on_error, uri=True, raw=False):
    """See :func:`ui_path_doc_stream` for documentation"""
    maybe_wrap = identity if raw else SimpleDocNav

    for fname in files:
        try:
            for p, doc in read_documents(fname, uri=uri):
                yield p, maybe_wrap(doc)

        except InvalidDocException as e:
            on_error(fname, e)
