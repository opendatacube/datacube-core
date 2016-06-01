from __future__ import absolute_import
from .version import get_version

__version__ = get_version()

from .api import Datacube # pylint: disable=wrong-import-position
