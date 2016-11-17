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


    from urllib.parse import urlparse, urljoin
    from urllib.request import url2pathname
    from itertools import zip_longest
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

    from urlparse import urlparse, urljoin
    from urllib import url2pathname
    from itertools import izip_longest as zip_longest


def with_metaclass(meta, *bases):
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return metaclass('temporary_class', None, {})
