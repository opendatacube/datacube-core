import os

import pathlib
import re
from typing import Optional, List, Union
import urllib.parse
from urllib.parse import urlparse, parse_qsl, urljoin
from urllib.request import url2pathname
from pathlib import Path

URL_RE = re.compile(r'\A\s*[\w\d\+]+://')


def is_url(url_str: str) -> bool:
    """
    Check if url_str tastes like a url (starts with blah://)

    >>> is_url('file:///etc/blah')
    True
    >>> is_url('http://greg.com/greg.txt')
    True
    >>> is_url('s3:///etc/blah')
    True
    >>> is_url('gs://data/etc/blah.yaml')
    True
    >>> is_url('/etc/blah')
    False
    >>> is_url('C:/etc/blah')
    False
    """
    try:
        return URL_RE.match(url_str) is not None
    except TypeError:
        return False


def uri_to_local_path(local_uri: Optional[str]) -> Optional[pathlib.Path]:
    """
    Transform a URI to a platform dependent Path.

    For example on Unix:
    'file:///tmp/something.txt' -> '/tmp/something.txt'

    On Windows:
    'file:///C:/tmp/something.txt' -> 'C:\\tmp\\test.tmp'

    .. note:
        Only supports file:// schema URIs
    """
    if not local_uri:
        return None

    components = urlparse(local_uri)
    if components.scheme != 'file':
        raise ValueError('Only file URIs currently supported. Tried %r.' % components.scheme)

    path = url2pathname(components.path)

    if components.netloc:
        if os.name == 'nt':
            path = '//{}{}'.format(components.netloc, path)
        else:
            raise ValueError('Only know how to use `netloc` urls on Windows')

    return pathlib.Path(path)


def mk_part_uri(uri: str, idx: int) -> str:
    """ Appends fragment part to the uri recording index of the part
    """
    return '{}#part={:d}'.format(uri, idx)


def get_part_from_uri(uri: str) -> Optional[int]:
    """
    Reverse of mk_part_uri

    returns None|int|string
    """

    def maybe_int(v):
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            return v

    opts = dict(parse_qsl(urlparse(uri).fragment))
    return maybe_int(opts.get('part', None))


def as_url(maybe_uri: str) -> str:
    if is_url(maybe_uri):
        return maybe_uri
    else:
        return pathlib.Path(maybe_uri).absolute().as_uri()


def default_base_dir() -> pathlib.Path:
    """Return absolute path to current directory. If PWD environment variable is
       set correctly return that, note that PWD might be set to "symlinked"
       path instead of "real" path.

       Only return PWD instead of cwd when:

       1. PWD exists (i.e. launched from interactive shell)
       2. Contains Absolute path (sanity check)
       3. Absolute ath in PWD resolves to the same directory as cwd (process didn't call chdir after starting)
    """
    cwd = pathlib.Path('.').resolve()

    _pwd = os.environ.get('PWD')
    if _pwd is None:
        return cwd

    pwd = pathlib.Path(_pwd)
    if not pwd.is_absolute():
        return cwd

    try:
        pwd_resolved = pwd.resolve()
    except IOError:
        return cwd

    if cwd != pwd_resolved:
        return cwd

    return pwd


def normalise_path(p: Union[str, pathlib.Path],
                   base: Optional[Union[str, pathlib.Path]] = None) -> pathlib.Path:
    """Turn path into absolute path resolving any `../` and `.`

       If path is relative pre-pend `base` path to it, `base` if set should be
       an absolute path. If not set, current working directory (as seen by the
       user launching the process, including any possible symlinks) will be
       used.
    """
    assert isinstance(p, (str, pathlib.Path))
    assert isinstance(base, (str, pathlib.Path, type(None)))

    def norm(p):
        return pathlib.Path(os.path.normpath(str(p)))

    if isinstance(p, str):
        p = pathlib.Path(p)

    if isinstance(base, str):
        base = pathlib.Path(base)

    if p.is_absolute():
        return norm(p)

    if base is None:
        base = default_base_dir()
    elif not base.is_absolute():
        raise ValueError("Expect base to be an absolute path")

    return norm(base / p)


def uri_resolve(base: str, path: Optional[str]) -> str:
    """
    path                  -- if path is a uri
    Path(path).as_uri()   -- if path is absolute filename
    base/path             -- in all other cases
    """
    if not path:
        return base

    if is_url(path):
        return path

    p = Path(path)
    if p.is_absolute():
        return p.as_uri()

    return urljoin(base, path)


def pick_uri(uris: List[str], scheme: Optional[str] = None) -> str:
    """ If scheme is supplied:
          Return first uri matching the scheme or raises Exception
        If scheme is not supplied:
          Return first `file:` uri, or failing that the very first uri
    """

    def pick(uris: List[str], scheme: str) -> Optional[str]:
        for uri in uris:
            if uri.startswith(scheme):
                return uri
        return None

    if len(uris) < 1:
        raise ValueError('No uris on a dataset')

    base_uri = pick(uris, scheme or 'file:')

    if base_uri is not None:
        return base_uri

    if scheme is not None:
        raise ValueError('No uri with required scheme was found')

    return uris[0]


def register_scheme(*schemes):
    """
    Register additional uri schemes as supporting relative offsets (etc), so that band/measurement paths can be
    calculated relative to the base uri.
    """
    urllib.parse.uses_netloc.extend(schemes)
    urllib.parse.uses_relative.extend(schemes)
    urllib.parse.uses_params.extend(schemes)


# s3:// not recognised by python by default
#  without this `urljoin` might be broken for s3 urls
register_scheme('s3')

# gs:// not recognised by python by default
#  without this `urljoin` might be broken for google storage urls
register_scheme('gs')
