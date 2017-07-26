# coding=utf-8
# We're using references that don't exist in python 3 (unicode, long):
# pylint: skip-file

"""
Compatibility helpers for Python 2 and 3.

See: http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/

"""
import sys

PY2 = sys.version_info[0] == 2

if not PY2:
    text_type = str
    string_types = (str,)
    integer_types = (int,)
    unicode_to_char = chr
    long_int = int

    # Lazy range function
    range = range

    import configparser

    NoOptionError = configparser.NoOptionError


    def read_config(default_text=None):
        config = configparser.ConfigParser()
        if default_text:
            config.read_string(default_text)
        return config


    # noinspection PyUnresolvedReferences
    from urllib.parse import urlparse, urljoin
    # noinspection PyUnresolvedReferences
    from urllib.request import url2pathname
    # noinspection PyUnresolvedReferences
    from itertools import zip_longest
    # noinspection PyUnresolvedReferences
    import urllib.parse as url_parse_module

    # Dynamic loading from filename varies across python versions
    # Based on http://stackoverflow.com/a/67692
    if sys.version_info >= (3, 5):
        # pylint: disable=import-error
        from importlib.util import spec_from_file_location, module_from_spec


        def load_mod(name, filepath):
            spec = spec_from_file_location(name, filepath)
            mod = module_from_spec(spec)
            spec.loader.exec_module(mod)
            sys.modules[spec.name] = mod
            return mod


        # pylint: disable=invalid-name
        load_module = load_mod
    elif sys.version_info[0] == 3:  # python 3.3, 3.4: untested
        # pylint: disable=import-error
        from importlib.machinery import SourceFileLoader

        # pylint: disable=invalid-name
        load_module = SourceFileLoader

else:
    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)
    unicode_to_char = unichr
    long_int = long

    # Lazy range function
    range = xrange

    import ConfigParser
    from io import StringIO

    NoOptionError = ConfigParser.NoOptionError


    def read_config(default_text=None):
        config = ConfigParser.SafeConfigParser()
        if default_text:
            config.readfp(StringIO(default_text))
        return config


    # noinspection PyUnresolvedReferences
    from urlparse import urlparse, urljoin
    # noinspection PyUnresolvedReferences
    from urllib import url2pathname
    # noinspection PyUnresolvedReferences
    from itertools import izip_longest as zip_longest
    # noinspection PyUnresolvedReferences
    import urlparse as url_parse_module

    import imp

    # pylint: disable=invalid-name
    load_module = imp.load_source


def with_metaclass(meta, *bases):
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return metaclass('temporary_class', None, {})
